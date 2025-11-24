import os
from contextlib import contextmanager

import logging
import psycopg2
from psycopg2.extras import DictCursor

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class DatabasePersistence:
    def __init__(self):
        self._setup_schema()
    
    def _setup_schema(self):
        with self._database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute('''
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = 'public' and table_name = 'assets'
                ''')
                if cursor.fetchone()[0] == 0:
                    cursor.execute('''
                        CREATE TABLE assets (
                            id serial PRIMARY KEY,
                            ticker text NOT NULL,
                            "name" text NOT NULL,
                            category text NOT NULL,
                            current_price NUMERIC(10, 2) NOT NULL
                        );
                    ''')
                
                cursor.execute('''
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'accounts';
                ''')
                if cursor.fetchone()[0] == 0:
                    cursor.execute('''
                        CREATE TABLE accounts (
                            id serial PRIMARY KEY,
                            account_name text NOT NULL,
                            account_type text NOT NULL
                        );
                    ''')
                
                cursor.execute('''
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'holdings';
                ''')
                if cursor.fetchone()[0] == 0:
                    cursor.execute('''
                        CREATE TABLE holdings (
                            asset_id integer NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
                            account_id integer NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                            shares integer NOT NULL DEFAULT 0
                        );
                    ''')
    
    @contextmanager
    def _database_connect(self):
        if os.environ.get('FLASK_ENV') == 'production':
            connection = psycopg2.connect(os.environ['DATABASE_URL'])
        else:
            connection = psycopg2.connect(dbname="portfolio")
        
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def all_holdings(self):
        query = '''
            SELECT
                accounts.account_name,
                accounts.account_type,
                assets.ticker,
                assets.name,
                assets.category,
                assets.current_price,
                holdings.shares,
                (assets.current_price * holdings.shares) AS market_value
            FROM accounts 
            JOIN holdings on accounts.id = holdings.account_id
            JOIN assets ON assets.id = holdings.asset_id
        '''
        logger.info('Executing query: %s', query)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                
        holdings = [dict(lst) for lst in results]
        return holdings

    def all_accounts(self):
        query = 'SELECT account_name, account_type FROM accounts'
        logger.info('Executing query: %s', query)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()

        accounts = [dict(lst) for lst in results]
        return accounts

    def all_assets(self):
        query = '''
            SELECT
                ticker, 
                name, 
                category, 
                current_price
            FROM assets'''
        logger.info('Executing query: %s', query)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            
        assets = [dict(lst) for lst in results]
        return assets

    def add_account(self, account_name, account_type):
        query = '''
            INSERT INTO accounts (account_name, account_type) 
            VALUES (%s, %s)
        '''
        logger.info('''Executing query: %s 
            with account_name: %s and account_type: %s
            ''', query, account_name, account_type
            )

        with self._database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (account_name, account_type))
        
    def add_asset(self, ticker, name, category, price):
        query = '''
            INSERT INTO assets (ticker, name, category, price) 
            VALUES (%s, %s, %s, %s)
        '''
        logger.info('''Executing query: %s 
            with ticker: %s, name: %s, category: %s, price: %s
            ''', ticker, name, category, price
            )
        
        with self._database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (ticker, name, category, current_price))

    def account_holdings(self, account_id):
        query = '''
            SELECT
                accounts.account_name,
                accounts.account_type,
                assets.ticker,
                assets.name,
                assets.category,
                assets.current_price,
                holdings.shares,
                (assets.current_price * holdings.shares) AS market_value
            FROM accounts 
            JOIN holdings on accounts.id = holdings.account_id
            JOIN assets ON assets.id = holdings.asset_id
            WHERE accounts.id = %s
        '''
        logger.info('Executing query: %s with account_id: %s', query, account_id)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, (account_id,))
                results = cursor.fetchall()
        
        account_holdings = [dict(lst) for lst in results]
        return account_holdings
