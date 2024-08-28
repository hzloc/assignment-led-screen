import random
import time
import unittest

from sqlalchemy import create_engine, text

mock_manual_data = [
    {
        "event_id": 1,
        "event_time": 1724792400000,

        "rate": 0.3435,
        "ccy_couple": "AUDUSD"
    },
    {
        "event_id": 2,
        "event_time": 1724792400000,
        "rate": 1.2435,
        "ccy_couple": "EURUSD"
    },
    {
        "event_id": 3,
        "event_time": 1724792400000,
        "rate": 0.0,
        "ccy_couple": "EURGBP"
    },
    {
        "event_id": 4,
        "event_time": 1724792400000,
        "rate": 0.5435,
        "ccy_couple": "GBPUSD"
    },
    {
        "event_id": 5,
        "event_time": 1724792400000,
        "rate": 0.5435,
        "ccy_couple": "NZDUSD"
    },
]


def generate_currency_updates_mock():
    event_id = random.randint(1, 10000000000000000)
    event_time = time.time_ns() // 1e6
    rate = random.uniform(0.5, 2.0)
    ccy_couple = random.choice(("AUDUSD", "EURGBP", "EURUSD", "GBPUSD", "NZDUSD"))
    return {
        "event_id": event_id,
        "event_time": event_time,
        "rate": rate,
        "ccy_couple": ccy_couple
    }


class MyTestCase(unittest.TestCase):
    def setUp(self):
        engine = create_engine("postgresql+psycopg2://example:example@localhost:54325/example")
        self.connection = engine.connect()

    def test_add_db_manual_values(self):
        for obs in mock_manual_data:
            self.connection.execute(text(
                "INSERT INTO rates(event_id, event_time, rate, ccy_couple) VALUES (:event_id, :event_time, :rate, :ccy_couple)"),
                obs)
            print("Inserting the mock change", obs)
            self.connection.commit()
        self.connection.close()

    def test_db_load(self):
        try:
            for i in range(10000000000):
                mock_update = generate_currency_updates_mock()
                print(mock_update)
                time.sleep(0.01)
                self.connection.execute(text(
                    "INSERT INTO rates(event_id, event_time, rate, ccy_couple) VALUES (:event_id, :event_time, :rate, :ccy_couple)"),
                    mock_update)
                print("Inserting the mock change", mock_update)
                self.connection.commit()
        except Exception as exc:
            print(exc)
        finally:
            self.connection.commit()
            self.connection.close()

    def tearDown(self):
        self.connection.close()

if __name__ == '__main__':
    unittest.main()
