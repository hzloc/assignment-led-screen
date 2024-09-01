import os
from time import sleep

import pendulum
from kafka import KafkaProducer

from src.logger import create_logger
from src.configs import *
from src.faker import create_mock_rate
from src.serializer import JSONSerializer

ENV = os.getenv("ENV", "local")
log = create_logger("producer")

if __name__ == "__main__":
    mock_start_date = pendulum.now(tz='America/New_York')

    if ENV == "local":
        boostrap_servers = LOCAL_BROKERS.replace('kafka://', '').split(";")
    else:
        boostrap_servers = BROKER.replace('kafka://', '').split(";")

    kafka_configs = {
        "bootstrap_servers": boostrap_servers,
        "client_id": "mock-rate-api",
        "value_serializer": lambda m: JSONSerializer.serialize(m),
        "key_serializer": lambda m: JSONSerializer.serialize(m)
    }
    rate_producer = KafkaProducer(**kafka_configs)
    if rate_producer.bootstrap_connected():
        log.info("Connected to the brokers successfully!")

    for i in range(40):
        # ms => minutes
        mock_start_date = mock_start_date.add(seconds=MOCK_RATE_GENERATOR_DELAY)
        mocks = [create_mock_rate(mock_start_date) for x in range(5)]
        rate_update_mock = create_mock_rate(mock_start_date)
        for mock in mocks:
            rate_producer.send(topic="rates", value=mock, key=mock['ccy_couple'])
            log.info(mock)
        sleep(MOCK_RATE_GENERATOR_DELAY)
