import os
import time
from typing import Dict, List
import pendulum
import pandas as pd
import schedule
from kafka import OffsetAndMetadata, TopicPartition
from kafka.consumer import KafkaConsumer
from sqlalchemy import create_engine, text

import concurrent.futures

from src.logger import log
from src.serializer.json_serializer import JSONSerializer

ENV = os.getenv("ENV", "local")
if ENV == "local":
    boostrap_servers = ["localhost:29092", "localhost:39092", "localhost:49092"]
    db_host = 'localhost'
    db_port = '5433'
else:
    boostrap_servers = ["broker-1:19092", "broker-2:19092", "broker-3:19092"]
    db_host = 'db'
    db_port = '5432'

ENGINE_URL = f'postgresql+psycopg2://realtime:realtime@{db_host}:{db_port}/realtime'


def create_led_view(raw_data: [dict]):
    df = pd.DataFrame(raw_data)
    df['now'] = pendulum.now("UTC").int_timestamp
    df['ccy_couple'] = df['ccy_couple'].apply(lambda v: v[:3] + '/' + v[-3:])
    # rates from last 30 seconds
    df = df.loc[(df['now'] - df['event_time']) <= 30, ['ccy_couple', 'rate']]

    return df


def insert_rates_to_db():
    log.info("#################")
    log.info("Starting daily insert at 5 PM New York Time")
    engine = create_engine(url=ENGINE_URL)
    connection = engine.connect()
    current_rates = run_fetch()
    log.info(f"Current exchange rates:\n {current_rates}")
    try:
        connection.execute(
            text(
                """INSERT INTO rates_at_five_pm(rate, ccy_couple) VALUES(:rate, :ccy_couple)"""),
            current_rates.to_dict(orient='records')
        )
    except Exception as exc:
        log.error(exc)
    finally:
        connection.close()


def run_fetch():
    rates = []
    dummy_consumer = KafkaConsumer(
        group_id="rate-dashboard",
        bootstrap_servers=boostrap_servers,
        value_deserializer=lambda m: JSONSerializer.deserialize(m),
        key_deserializer=lambda m: JSONSerializer.deserialize(m),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )
    if dummy_consumer.bootstrap_connected():
        log.info("Consumer connected successfully!")
    topic = 'rates'
    partitions = dummy_consumer.partitions_for_topic(topic=topic)
    topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for futures in executor.map(fetch_and_transform_currency_rates, topic_partitions):
            if futures is not None:
                rates.append(futures)
    rates_df = create_led_view(raw_data=rates)
    print(rates_df)
    return rates_df


def fetch_and_transform_currency_rates(tp) -> pd.DataFrame:
    # Kafka consumer configuration
    consumer = KafkaConsumer(
        group_id="rate-dashboard",
        bootstrap_servers=boostrap_servers,
        value_deserializer=lambda m: JSONSerializer.deserialize(m),
        key_deserializer=lambda m: JSONSerializer.deserialize(m),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )
    consumer.assign([tp])
    consumer.seek_to_end(tp)
    position = consumer.position(tp)
    if position > 0:
        consumer.poll()
        consumer.seek_to_end()
        msg = consumer.poll(timeout_ms=30000)  # Poll the last message
        log.debug(msg)
        return msg[tp][0].value
    else:
        consumer.close()
        return


log.info("Led conversition table - minutely refreshed rates")
schedule.every(1).days.do(run_fetch)
schedule.every(1).days.at(time_str="17:00", tz="America/New_York").do(insert_rates_to_db)

if __name__ == '__main__':
    while True:
        schedule.run_pending()
        time.sleep(1)
