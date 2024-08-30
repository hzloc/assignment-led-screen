import os

from kafka import KafkaProducer
from time import sleep
from src.faker import create_mock_rate
from src.logger import log
from src.serializer import JSONSerializer

ENV = os.getenv("ENV", "local")
print(ENV)

if __name__ == "__main__":
    if ENV == "local":
        boostrap_servers = ["localhost:29092", "localhost:39092", "localhost:49092"]
    else:
        boostrap_servers = ["broker-1:9092", "broker-2:9092", "broker-3:9092"]

    kafka_configs = {
        "bootstrap_servers": boostrap_servers,
        "client_id": "mock-rate-api",
        "value_serializer": lambda m: JSONSerializer.serialize(m),
        "key_serializer": lambda m: JSONSerializer.serialize(m)
    }
    rate_producer = KafkaProducer(**kafka_configs)
    if rate_producer.bootstrap_connected():
        log.info("Connected to the brokers successfully!")

    while True:
        rate_update_mock = create_mock_rate()
        rate_producer.send(topic="rates", value=rate_update_mock, key=rate_update_mock['ccy_couple'])
