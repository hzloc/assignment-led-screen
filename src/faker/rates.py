import random
import time
import pendulum

def create_mock_rate() -> dict:
    event_id = random.randint(1, 10000000000000000)
    event_time = pendulum.now("UTC").int_timestamp
    rate = random.uniform(0.5, 2.0)
    ccy_couple = random.choice(("AUDUSD", "EURGBP", "EURUSD", "GBPUSD", "NZDUSD"))
    return {
        "event_id": event_id,
        "event_time": event_time,
        "rate": rate,
        "ccy_couple": ccy_couple
    }
