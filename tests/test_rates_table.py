from sqlalchemy import create_engine
from sqlalchemy import text
import unittest

class TestDb(unittest.TestCase):

    def setUp(self):
        engine = create_engine("postgresql+psycopg2://example:example@localhost:54325/example")
        self.connection = engine.connect()

    def test_postgres_table(self):
        results = self.connection.execute(statement=text("SELECT * FROM rates"))
        print(results.fetchmany(5))
        self.connection.close()
    def tearDown(self):
        self.connection.close()


if __name__ == '__main__':
    unittest.main()
