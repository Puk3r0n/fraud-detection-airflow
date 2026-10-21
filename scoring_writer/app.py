import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "scoring-writer")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
FLUSH_INTERVAL = float(os.getenv("FLUSH_INTERVAL", "2.0"))

DSN = (
    f"host={os.getenv('POSTGRES_HOST', 'postgres')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')} "
    f"user={os.getenv('POSTGRES_USER', 'fraud')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'fraud')} "
    f"dbname={os.getenv('POSTGRES_DB', 'fraud')}"
)

UPSERT = """
INSERT INTO scores (transaction_id, score, fraud_flag, model_version)
VALUES %s
ON CONFLICT (transaction_id) DO UPDATE
SET score = EXCLUDED.score,
    fraud_flag = EXCLUDED.fraud_flag,
    model_version = EXCLUDED.model_version,
    scored_at = now()
"""


def connect_with_retry(attempts=30, delay=2.0):
    for attempt in range(1, attempts + 1):
        try:
            conn = psycopg2.connect(DSN)
            conn.autocommit = False
            logger.info("Connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as exc:
            logger.warning("PostgreSQL not ready (%s/%s): %s", attempt, attempts, exc)
            time.sleep(delay)
    raise RuntimeError("PostgreSQL is not reachable")


def flush(conn, consumer, rows):
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, UPSERT, rows)
    conn.commit()
    consumer.commit(asynchronous=False)
    logger.info("Wrote %s scores to PostgreSQL", len(rows))
    return len(rows)


def main():
    conn = connect_with_retry()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([SCORING_TOPIC])
    logger.info("Listening topic '%s', writing to PostgreSQL", SCORING_TOPIC)

    rows = []
    last_flush = time.monotonic()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Kafka error: %s", msg.error())
                else:
                    try:
                        payload = json.loads(msg.value().decode("utf-8"))
                        rows.append((
                            payload["transaction_id"],
                            float(payload["score"]),
                            int(payload["fraud_flag"]),
                            str(payload["model_version"]),
                        ))
                    except (ValueError, KeyError, TypeError):
                        logger.exception("Skipping malformed message")

            expired = time.monotonic() - last_flush >= FLUSH_INTERVAL
            if len(rows) >= BATCH_SIZE or (rows and expired):
                flush(conn, consumer, rows)
                rows = []
                last_flush = time.monotonic()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    finally:
        try:
            flush(conn, consumer, rows)
        except Exception:
            logger.exception("Final flush failed")
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
