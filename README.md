# Fraud Detection: переобучение в Airflow

ДЗ 3 по ML System Design. Сервис скоринга из [ДЗ 1](https://github.com/Puk3r0n/fraud-detection-realtime) плюс Airflow:

- supply_labeled_batch - дописывает в витрину новый размеченный месяц;
- retrain_fraud_model - сдвигает окно обучения, учит кандидата и публикует его, если PR-AUC не хуже прод-модели;
- data_quality_report - проверяет витрину и строит HTML-отчёт.

fraud_detector подхватывает новую модель без перезапуска.

## Запуск

    cp .env.example .env
    mkdir -p data    # train.csv и test.csv с Kaggle
    docker compose up -d --build

- Airflow: http://localhost:8081, admin / admin
- Kafka UI: http://localhost:8080
- DQ-отчёт: artifacts/dq_reports/latest.html

## Проверка

    docker compose exec airflow airflow dags trigger retrain_fraud_model
    cat artifacts/models/latest.txt
    docker compose logs fraud_detector | grep loaded
    docker compose exec postgres psql -U fraud -d fraud -c \
      "SELECT model_version, count(*) FROM scores GROUP BY 1"
