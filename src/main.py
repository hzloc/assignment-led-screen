import os

import faust
import pendulum

from src.configs import BROKER, LOCAL_BROKERS, NA_VALUE_FLOAT, NEW_YORK_TIME_ZONE
from src.logger import create_file_logger
from src.records import FXRate, LedTable

ENV = os.getenv("ENV", 'local')

if ENV == "local":
    boostrap_servers = LOCAL_BROKERS
else:
    boostrap_servers = BROKER

app = faust.App('fx-exchange-tracker', broker=boostrap_servers, store="rocksdb://")

rates_stream = app.topic("rates", value_type=FXRate)
live_led_table_stream = app.topic("live_led_table", value_type=LedTable, key_type=str)

led_table_view_exchanges = app.Table("led-table-exchanges")
led_table_view_exchanges_last_updated = app.Table('led-table-exchanges-last-updated', default=float)

everyday_fx_pair_rates_at_5pm_ny_time = app.Table('everyday_fx_pair_rates_at_5pm_ny_time', default=float)
yesterday_fx_pair_rates_at_5pm_ny_time = app.Table('yesterday_fx_pair_rates_at_5pm_ny_time', default=float)
last_updated_fx_rates_timestamp = app.Table('last_updated_fx_rates_timestamp', default=float)


@app.agent(rates_stream)
async def update_yesterday_rates(exchange_rates):
    """
    At 17:00 PM New York time,
        -> #everyday_fx_pair_rates_at_5pm_ny_time[ccy_couple] => rates
        -> #last_updated_fx_rates_timestamp[ccy_couple] => today
    Conditionally just before updating today's rate with new values, yesterday_fx_pair_rates_at_5pm_ny_time
    table will update its keys if their timestamp corresponds to the yesterday's timestamp.

    :param exchange_rates: continuous stream of the exchange rates
    :return:
    """
    async for exchange_rate in exchange_rates.group_by(FXRate.ccy_couple):
        update_timestamp = exchange_rate.event_time / 1000
        today = pendulum.today(tz=NEW_YORK_TIME_ZONE).at(hour=17).float_timestamp
        yesterday = (pendulum.yesterday(tz=NEW_YORK_TIME_ZONE).at(hour=17).float_timestamp)
        # every day at 5 PM New York Time
        if today == update_timestamp:
            if (
                    (exchange_rate.ccy_couple in everyday_fx_pair_rates_at_5pm_ny_time.keys()) &
                    (last_updated_fx_rates_timestamp[exchange_rate.ccy_couple] == yesterday)
            ):
                yesterday_fx_pair_rates_at_5pm_ny_time[exchange_rate.ccy_couple] = \
                    everyday_fx_pair_rates_at_5pm_ny_time[exchange_rate.ccy_couple]
            else:
                yesterday_fx_pair_rates_at_5pm_ny_time[exchange_rate.ccy_couple] = NA_VALUE_FLOAT
            everyday_fx_pair_rates_at_5pm_ny_time[exchange_rate.ccy_couple] = exchange_rate.rate
            last_updated_fx_rates_timestamp[exchange_rate.ccy_couple] = today


@app.agent(rates_stream)
async def live_led_table(exchange_rates):
    async for exchange_rate in exchange_rates:
        """
        Transformation + Publish
        
        :param exchange_rates: continuous stream of the exchange rates
        """
        # i.e: EURAUD => EUR/AUD
        ccy_couple = exchange_rate.ccy_couple[:3] + "/" + exchange_rate.ccy_couple[-3:]
        current_rate = exchange_rate.rate
        change = ""
        timestamp = exchange_rate.event_time / 1000
        exchange_updates = {"ccy_couple": ccy_couple, "rate": current_rate, "change": change, "timestamp": timestamp}
        if (
                (exchange_rate.ccy_couple in yesterday_fx_pair_rates_at_5pm_ny_time.keys()) &
                (yesterday_fx_pair_rates_at_5pm_ny_time[exchange_rate.ccy_couple] != NA_VALUE_FLOAT)
        ):
            yesterdays_rate = yesterday_fx_pair_rates_at_5pm_ny_time[exchange_rate.ccy_couple]
            change = f"{(current_rate - yesterdays_rate) / yesterdays_rate * 100:.3f}%"
            exchange_updates["change"] = change
        await live_led_table_stream.send(key=ccy_couple, value=exchange_updates, key_serializer='json',
                                         value_serializer='json')


@app.agent(live_led_table_stream)
async def live_exchange_rates_table_updates(exchange_rate_updates):
    """
    Logs active rates to the console

    :param exchange_rate_updates: continuous stream of the exchange rates enriched with changes
    :return:
    """
    async for exchange_rate_update in exchange_rate_updates:
        current_time = pendulum.now(tz=NEW_YORK_TIME_ZONE).float_timestamp
        if (current_time - exchange_rate_update.timestamp) <= 30.0:
            ccy_couple = exchange_rate_update.ccy_couple
            rate = exchange_rate_update.rate
            change = exchange_rate_update.change
            # print(f'"{ccy_couple}",{rate:.5f},{change}')


@app.agent(live_led_table_stream)
async def update_live_led_table(exchange_rate_updates):
    """
    Updates the table for the rates
    :param exchange_rate_updates:
    :return:
    """
    async for exchange_rate_update in exchange_rate_updates.group_by(LedTable.ccy_couple):
        current_time = pendulum.now(tz=NEW_YORK_TIME_ZONE).float_timestamp
        if (current_time - exchange_rate_update.timestamp) <= 30.0:
            ccy_couple = exchange_rate_update.ccy_couple
            rate = exchange_rate_update.rate
            change = exchange_rate_update.change
            led_table_view_exchanges[exchange_rate_update.ccy_couple] = f'"{ccy_couple}",{rate:.5f},{change}'
            led_table_view_exchanges_last_updated[exchange_rate_update.ccy_couple] = current_time
        else:
            led_table_view_exchanges[exchange_rate_update.ccy_couple] = f''


@app.timer(60)
async def print_live_updates_table():
    now = pendulum.now(tz=NEW_YORK_TIME_ZONE).float_timestamp
    print("##################")
    print("currency,rate,change")
    for currency, timestamp in led_table_view_exchanges_last_updated.items():
        if now - timestamp < 30:
            print(f"{led_table_view_exchanges[currency]}")
        else:
            print(f'{currency} - Not active')
    print("##################")


if __name__ == "__main__":
    app.main()
