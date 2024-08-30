import boto3
import pendulum
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import Decimal
from botocore.errorfactory import ClientError

import logging

from src.faker.rates import generate_currency_updates_mock

logger = logging.getLogger("logger")


class RatesTableDB:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
        self.table = None
        self.table_name = 'rates'

    def create_table_rates(self):
        table_config = {
            "AttributeDefinitions": [
                {
                    "AttributeName": 'ccy_couple',
                    'AttributeType': 'S'
                },
                {
                    "AttributeName": 'event_time',
                    'AttributeType': 'N'
                },
                {
                    "AttributeName": 'from',
                    'AttributeType': 'S'
                }
            ],
            "TableName": self.table_name,
            "KeySchema": [
                {
                    "AttributeName": 'ccy_couple',
                    'KeyType': 'HASH'
                },
                {
                    "AttributeName": 'event_time',
                    'KeyType': 'RANGE'
                },
            ],
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "ccyFromGSI",
                    "Projection": {
                        'ProjectionType': 'ALL',
                    },
                    "KeySchema": [
                        {
                            "AttributeName": 'from',
                            'KeyType': 'HASH'
                        },
                        {
                            "AttributeName": 'event_time',
                            'KeyType': 'RANGE'
                        },
                    ]
                }
            ],
            'BillingMode': 'PAY_PER_REQUEST'
        }
        try:
            self.dynamodb.create_table(**table_config)
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", self.table_name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
        except Exception as err:
            print(err)
            self.table = self.dynamodb.Table(self.table_name)
        else:
            return self.table

    def write_to_rates(self, rate: dict):
        if rate is None:
            return
        table = self.dynamodb.Table("rates")

        rate['ccy_couple'] = rate['ccy_couple'][:3] + '/' + rate['ccy_couple'][-3:]
        rate['event_id'] = Decimal(str(rate['event_id']))
        rate['event_time'] = rate['event_time']
        rate['rate'] = Decimal(str(rate['rate']))
        rate['from'] = rate['ccy_couple'][:3]
        rate['to'] = rate['ccy_couple'][-3:]
        print(rate)
        table.put_item(Item=rate)

    def query_using_updates_on(self, ccy_group: str, last_seconds=30):
        table = self.dynamodb.Table(self.table_name)
        thirty_seconds_previous_date = pendulum.now(tz="UTC").subtract(seconds=last_seconds).int_timestamp
        results = table.query(KeyConditionExpression=
                              (Key("ccy_couple").eq(ccy_group) & Key('event_time').gt(thirty_seconds_previous_date)))
        return results

    def query_from_rates(self, currency_name: str = "EUR", last_seconds=30):
        currency_name = currency_name.upper()

        table = self.dynamodb.Table(self.table_name)
        thirty_seconds_previous_date = pendulum.now(tz="UTC").subtract(seconds=last_seconds).int_timestamp
        results = table.query(KeyConditionExpression=(Key("from").eq(currency_name) & Key('event_time')
                                                      .gt(thirty_seconds_previous_date)),
                              IndexName='ccyFromGSI'
                              )
        return results

    def query_from_rates_on_yesterday_at_5pm(self, currency_name):
        currency_name = currency_name.upper()
        table = self.dynamodb.Table(self.table_name)
        yesterday_date = pendulum.today(tz="America/New_York").subtract(days=1)
        yesterday_datetime_5pm = yesterday_date.set(hour=17, minute=0, second=0)
        yesterday_datetime_in_tz = yesterday_datetime_5pm.in_tz(tz="UTC")
        ny_yesterday_5pm_in_epochs = yesterday_datetime_in_tz.int_timestamp
        print(yesterday_datetime_5pm, yesterday_datetime_in_tz)
        results = table.query(KeyConditionExpression=(Key("from").eq(currency_name) & Key('event_time')
                                                      .eq(ny_yesterday_5pm_in_epochs)),
                              IndexName='ccyFromGSI'
                              )
        return results



if __name__ == "__main__":
    db = RatesTableDB()
    print(db.query_from_rates_on_yesterday_at_5pm("EUR"))
    print(db.query_using_updates_on("AUD/USD"), 30)
    print(db.query_from_rates('EUR', 30))
