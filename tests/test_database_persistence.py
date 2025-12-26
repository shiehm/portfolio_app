import os
from portfolio.database_persistence import DatabasePersistence
import psycopg2
import unittest
from werkzeug.security import check_password_hash

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

    # def tearDown(self):
    #     self._reset_all_tables()

    def _create_test_user(self):
        db = DatabasePersistence(user_id=None)
        db.create_user(username="test_user", password="test_password")
        with self._connection as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT id FROM users.users WHERE username = %s', ("test_user",))
                (user_id, ) = cursor.fetchone()
        return user_id
    
    # Test test_user created successfully
    def test_all_users_includes_created_user(self):
        users = self.db.all_users()
        self.assertEqual(["test_user"], users)
    
    # Test signin process works: user_id is correctly pulled and password matched 
    def test_load_user_credentials(self):
        credentials, user_ids = self.db.load_user_credentials()
        self.assertEqual(self.user_id, user_ids["test_user"])
        self.assertTrue(check_password_hash(credentials["test_user"], "test_password"))

    # Test structure of databases (columns) are as expected for the main views
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
        holdings, actual_holdings_columns = self.db.all_holdings()
        self.assertEqual(expected_holdings_columns, actual_holdings_columns)

    def test_account_totals_columns(self):
        expected_accounts_columns = [
            'account_name',
            'account_type',
            'account_id',
            'number_holdings',
            'total_market_value',
            'percent'
        ]
        accounts, actual_accounts_columns = self.db.account_totals()
        self.assertEqual(expected_accounts_columns, actual_accounts_columns)

    def test_asset_totals_columns(self):
        expected_assets_columns = [
            'id',
            'ticker',
            'name',
            'category',
            'current_price',
            'total_shares',
            'accounts_holding',
            'total_market_value',
            'percent',
        ]
        assets, actual_assets_columns = self.db.asset_totals()
        self.assertEqual(expected_assets_columns, actual_assets_columns)

    # Test insertion of accounts, assets, and holdings
    def test_add_account(self):
        self.db.add_account(account_name="test_account", account_type="test_type")
        test_account = self.db.all_accounts()[0]
        self.assertEqual(test_account['account_name'], "test_account")
        self.assertEqual(test_account['account_type'], "test_type")
    
    def test_add_asset(self):
        self.db.add_asset(ticker="TEST", name="test_name", category="test_category", current_price=10)
        test_asset = self.db.all_assets()[0]
        self.assertEqual(test_asset['ticker'], "TEST")
        self.assertEqual(test_asset['name'], "test_name")
        self.assertEqual(test_asset['category'], "test_category")
        self.assertEqual(test_asset['current_price'], 10)

    def test_add_holding(self):
        self.db.add_account("test_account", "test_type")
        self.db.add_asset("TEST", "test_name", "test_category", 10)
        self.db.add_holding(account_id=1, asset_id=1, shares=100)
        test_holding = self.db.find_holding(1)
        self.assertEqual(test_holding['account_name'], 'test_account')
        self.assertEqual(test_holding['account_type'], 'test_type')
        self.assertEqual(test_holding['ticker'], 'TEST')
        self.assertEqual(test_holding['name'], 'test_name')
        self.assertEqual(test_holding['category'], 'test_category')
        self.assertEqual(test_holding['current_price'], 10)
        self.assertEqual(test_holding['account_id'], 1)
        self.assertEqual(test_holding['asset_id'], 1)
        self.assertEqual(test_holding['shares'], 100)
    
    # Test deletion of accounts, assets, and holdings
    def test_delete_account(self):
        self.db.add_account(account_name="test_account", account_type="test_type")
        self.db.delete_account(1)
        test_account = self.db.all_accounts()
        self.assertEqual([], test_account)
    
    def test_delete_asset(self):
        self.db.add_asset(ticker="TEST", name="test_name", category="test_category", current_price=10)
        self.db.delete_asset(1)
        test_asset = self.db.all_assets()
        self.assertEqual([], test_asset)

    def test_delete_holding(self):
        self.db.add_account("test_account", "test_type")
        self.db.add_asset("TEST", "test_name", "test_category", 10)
        self.db.add_holding(account_id=1, asset_id=1, shares=100)
        self.db.delete_holding(1)
        test_holding, columns = self.db.all_holdings()
        self.assertEqual([], test_holding)

    # Testing account specific view
    def test_account_holdings(self):
        self.db.add_account("test_account", "test_type")
        self.db.add_asset("TEST", "test_name", "test_category", 10)
        self.db.add_holding(account_id=1, asset_id=1, shares=100)
        test_holdings, columns = self.db.account_holdings(1)
        test_holding = test_holdings[0]
        self.assertEqual(test_holding['account_name'], 'test_account')
        self.assertEqual(test_holding['account_type'], 'test_type')
        self.assertEqual(test_holding['ticker'], 'TEST')
        self.assertEqual(test_holding['name'], 'test_name')
        self.assertEqual(test_holding['category'], 'test_category')
        self.assertEqual(test_holding['current_price'], 10)
        self.assertEqual(test_holding['account_id'], 1)
        self.assertEqual(test_holding['asset_id'], 1)
        self.assertEqual(test_holding['shares'], 100)

    # Test update functions
    def test_update_asset(self):
        self.db.add_asset(ticker="TEST", name="test_name", category="test_category", current_price=10)
        self.db.update_asset(1, 99)
        test_asset = self.db.all_assets()[0]
        self.assertEqual(test_asset['ticker'], "TEST")
        self.assertEqual(test_asset['name'], "test_name")
        self.assertEqual(test_asset['category'], "test_category")
        self.assertEqual(test_asset['current_price'], 99)

    def test_update_holding(self):
        self.db.add_account("test_account", "test_type")
        self.db.add_asset("TEST", "test_name", "test_category", 10)
        self.db.add_holding(account_id=1, asset_id=1, shares=100)
        self.db.update_holding(1, 999)
        test_holdings, columns = self.db.account_holdings(1)
        test_holding = test_holdings[0]
        self.assertEqual(test_holding['account_name'], 'test_account')
        self.assertEqual(test_holding['account_type'], 'test_type')
        self.assertEqual(test_holding['ticker'], 'TEST')
        self.assertEqual(test_holding['name'], 'test_name')
        self.assertEqual(test_holding['category'], 'test_category')
        self.assertEqual(test_holding['current_price'], 10)
        self.assertEqual(test_holding['account_id'], 1)
        self.assertEqual(test_holding['asset_id'], 1)
        self.assertEqual(test_holding['shares'], 999)

if __name__ == '__main__':
    unittest.main()