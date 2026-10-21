import json
import logging
import os
import sys

import pandas as pd
from confluent_kafka import Consumer, Producer

sys.path.append(os.path.abspath("./src"))
from scorer import Scorer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("/app/logs/service.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "ml-scorer")


class ProcessingService:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
        })
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        self.scorer = Scorer()

    def handle(self, raw_value):
        message = json.loads(raw_value)
        result, model_version = self.scorer.predict(pd.DataFrame([message["data"]]))
        row = result.iloc[0]
        return {
            "transaction_id": message["transaction_id"],
            "score": float(row["score"]),
            "fraud_flag": int(row["fraud_flag"]),
            "model_version": model_version,
        }

    def run(self):
        logger.info("Listening topic '%s', writing to '%s'", TRANSACTIONS_TOPIC, SCORING_TOPIC)
        scored = 0
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Kafka error: %s", msg.error())
                continue
            try:
                output = self.handle(msg.value().decode("utf-8"))
                self.producer.produce(
                    SCORING_TOPIC,
                    key=output["transaction_id"],
                    value=json.dumps(output),
                )
                self.producer.poll(0)
                scored += 1
                if scored % 100 == 0:
                    logger.info("Scored %s messages, last: %s", scored, output)
            except Exception:
                logger.exception("Error processing message")

    def close(self):
        self.producer.flush()
        self.consumer.close()


if __name__ == "__main__":
    logger.info("Starting Kafka ML scoring service...")
    service = ProcessingService()
    try:
        service.run()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    finally:
        service.close()
