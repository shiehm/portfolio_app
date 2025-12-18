import os
from portfolio.database_persistence import DatabasePersistence
import psycopg2
import unittest

os.environ["FLASK_ENV"] = "test"

class DatabasePersistenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._connection = psycopg2.connect(dbname='test_portfolio') # raw connection to the test DB for cleanup
        _ = DatabasePersistence(user_id=None) # trigger schema setup once

    @classmethod
    def tearDownClass(cls):
        cls._connection.close()

    def _reset_all_tables(self):
        with self._connection:
            with self._connection.cursor() as cursor:
                cursor.execute("""
                    TRUNCATE TABLE accounts, assets, holdings, users.users
                    RESTART IDENTITY CASCADE;
                """)

    def setUp(self):
        self._reset_all_tables()
        self.user_id = self._create_test_user()
        self.db = DatabasePersistence(self.user_id)

    def tearDown(self):
        self._reset_all_tables()

    def _create_test_user(self, username="test_user", password="test_password"):
        db = DatabasePersistence(user_id=None)
        db.create_user(username, password)
        print("_create_test_user: user created")

        with self._connection as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT id FROM users.users WHERE username = %s', (username,))
                (user_id, ) = cursor.fetchone()
        return user_id
    
    def test_all_users_includes_created_user(self):
        users = self.db.all_users()
        self.assertEqual(["test_user"], users)
    
    # Test structure of databases (columns) are as expected
    def test_holdings_columns(self):
        expected_holdings_columns = [
            'account_name', 
            'account_type', 
            'ticker', 
            'name', 
            'category', 
            'current_price', 
            'shares', 
            'market_value', 
            'percent', 
            'asset_id', 
            'account_id', 
            'holding_id', 
            'user_id',
        ]
        actual_holdings_columns = self.db.get_columns()
        self.assertEqual(expected_holdings_columns, actual_holdings_columns)

if __name__ == '__main__':
    unittest.main()