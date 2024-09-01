import os

import faust
import pendulum
from pendulum import duration

from src.configs import LOCAL_BROKERS, NEW_YORK_TIME_ZONE
from src.logger import create_file_logger
from src.records import FXRate, LedTable

BROKER_URL = os.getenv("BROKER_URL", LOCAL_BROKERS)

app = faust.App('fx-exchange-tracker', broker=BROKER_URL)

log = create_file_logger("faust-rates")
rates_stream = app.topic("rates", value_type=FXRate)
live_led_table_stream = app.topic("live_led_table", value_type=LedTable)

exchange_rates_yesterday_rates_table = app.Table("rates-yesterday-test-at-5pm-newyorktz", default=dict)
exchange_rates_daily_at_5pm_ny_tz = app.Table("rates-every-day-at-5pm-newyorktz", default=dict).tumbling(
    size=duration(seconds=5), expires=duration(seconds=5)
)

@app.agent(rates_stream)
async def store_exchange_rates_at_5pm_every_day(exchange_rates):
    async for exchange_rate in exchange_rates.group_by(FXRate.ccy_couple):
        exchange_rate_datetime = exchange_rate.event_time / 1000
        current_rate = exchange_rate.rate
        today_5pm = pendulum.today(tz=NEW_YORK_TIME_ZONE).at(hour=17, minute=0, microsecond=0).float_timestamp
        if today_5pm == exchange_rate_datetime:
            exchange_rates_daily_at_5pm_ny_tz[exchange_rate.ccy_couple] = {"rate": current_rate, "timestamp": exchange_rate_datetime}
            print(exchange_rates_daily_at_5pm_ny_tz.as_ansitable())

@app.agent(rates_stream)
async def add_currency_exchange_rates_to_table(rates):
    async for rate_update in rates.group_by(FXRate.ccy_couple):
        yesterday = pendulum.yesterday(tz="America/New_York").at(hour=17, minute=0, second=0).float_timestamp
        event_time = rate_update.event_time / 1000
        exchange_rate = rate_update.rate
        if (event_time == yesterday) & (rate_update.rate != 0):
            exchange_rates_yesterday_rates_table[rate_update.ccy_couple] = {"rate": exchange_rate, "timestamp": event_time}
            print(f"{exchange_rates_yesterday_rates_table.as_ansitable()}")



@app.agent(rates_stream)
async def live_led_table(exchange_rates):
    async for exchange_rate in exchange_rates:
        # i.e: EURAUD => EUR/AUD
        ccy_couple = exchange_rate.ccy_couple[:3] + "/" + exchange_rate.ccy_couple[-3:]
        current_rate = exchange_rate.rate
        change = ""
        exchange_updates = {"ccy_couple": ccy_couple, "rate": current_rate, "change": change}
        if exchange_rate.ccy_couple in exchange_rates_yesterday_rates_table.keys():
            yesterdays_rate = exchange_rates_yesterday_rates_table[exchange_rate.ccy_couple].get('rate')
            change = f"{(current_rate - yesterdays_rate) / yesterdays_rate * 100:.3f}%"
            exchange_updates["change"] = change
        await live_led_table_stream.send(key=ccy_couple, value=exchange_updates, key_serializer='json',
                                         value_serializer='json')


@app.agent(live_led_table_stream)
async def live_exchange_rates_table_updates(exchange_rate_updates):
    async for exchange_rate_update in exchange_rate_updates:
        ccy_couple = exchange_rate_update.ccy_couple
        rate = exchange_rate_update.rate
        change = exchange_rate_update.change
        # print(f'"{ccy_couple}",{rate:.5f},{change}')


if __name__ == "__main__":
    app.main()
