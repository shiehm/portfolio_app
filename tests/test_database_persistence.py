import os
from portfolio.database_persistence import DatabasePersistence
import psycopg2
import unittest

class DatabasePersistenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ["FLASK_ENV"] = "test"
        cls._connection = psycopg2.connect(dbname='test_portfolio') # raw connection to the test DB for cleanup
        _ = DatabasePersistence(user_id=None) # trigger schema setup once

    @classmethod
    def tearDownClass(cls):
        cls._connection.close()

    def _reset_all_tables(self):
        with self._connection.cursor() as cursor:
            cursor.execute("""
                TRUNCATE TABLE accounts, assets, holdings, users.users
                RESTART IDENTITY CASCADE;
            """)

    def setUp(self):
        # Actually we will need to use create_user for every process that requires it. 
        self.user_id = 1
        self.db = DatabasePersistence(self.user_id)
        self._reset_all_tables()

    def tearDown(self):
        self._reset_all_tables()

    # Test connection works, databases are all connected 
    def test_connection(self):
        pass
        # self.db.all_users()
    
    # Test structure of databases (columns) are as expected
    def test_columns(self):
        pass

    # Test that various create/edit methods works
    def test_update(self):
        pass
    
    def tearDown(self):


if __name__ == '__main__':
    unittest.main()