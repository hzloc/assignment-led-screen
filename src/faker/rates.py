import random
import time
import pendulum


def create_mock_rate(start_mocking_from_date: pendulum.DateTime) -> dict:
    event_id = random.randint(1, 10000000000000000)
    # time * 1000 is due to the compability with rates_sample.csv
    event_time = int(start_mocking_from_date.float_timestamp * 1000)
    rate = random.uniform(0.5, 2.0)
    ccy_couple = random.choice(("AUDUSD", "EURGBP", "EURUSD", "GBPUSD", "NZDUSD"))
    return {
        "event_id": event_id,
        "event_time": event_time,
        "rate": rate,
        "ccy_couple": ccy_couple
    }
