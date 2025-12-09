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
        self._BASE_HOLDINGS_QUERY = 'SELECT * FROM views.base_holdings '
    
    def _base_holdings_query(self, filter=None):
        # Only shows accounts with holdings 
        if filter == 'has_holdings':
            filter_clause = 'WHERE shares IS NOT NULL '
            return self._BASE_HOLDINGS_QUERY + filter_clause

        # Shows all accounts regardless of holdings
        return self._BASE_HOLDINGS_QUERY
    
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
                            id serial PRIMARY KEY,
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

    def get_columns(self):
        filter_clause = 'LIMIT 0'
        query = self._base_holdings_query() + filter_clause
        logger.info('Executing query: %s', query)
        with self._database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                if cursor.description is None:
                    return []     
                # cursor.description is a list of tuples (name, type_code, ...)
                column_names = [column[0] for column in cursor.description]
        
        return column_names

    def all_holdings(self):
        query = self._base_holdings_query('has_holdings')
        logger.info('Executing query: %s', query)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                
        holdings = [dict(lst) for lst in results]
        return holdings

    def all_accounts(self):
        query = 'SELECT * FROM accounts'
        logger.info('Executing query: %s', query)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()

        accounts = [dict(lst) for lst in results]
        return accounts

    def all_assets(self):
        query = 'SELECT * FROM assets'
        logger.info('Executing query: %s', query)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            
        assets = [dict(lst) for lst in results]
        return assets
    
    def find_holding(self, holding_id):
        filter_clause = 'AND holding_id = %s'
        query = self._base_holdings_query('has_holdings') + filter_clause
        logger.info('Executing query: %s with holding_id: %s', query, holding_id)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, (holding_id,))
                result = cursor.fetchone()

        return result

    def find_asset(self, asset_id):
        query = 'SELECT * FROM assets WHERE id = %s'
        logger.info('Executing query: %s with id: %s', query, asset_id)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, (asset_id,))
                result = cursor.fetchone()

        return result

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
        
    def add_asset(self, ticker, name, category, current_price):
        query = '''
            INSERT INTO assets (ticker, name, category, current_price) 
            VALUES (%s, %s, %s, %s)
        '''
        logger.info('''Executing query: %s 
            with ticker: %s, name: %s, category: %s, current_price: %s
            ''', ticker, name, category, current_price
            )
        
        with self._database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (ticker, name, category, current_price))
    
    def add_holding(self, account_id, asset_id, shares):
        query = '''
            INSERT INTO holdings (account_id, asset_id, shares) 
            VALUES (%s, %s, %s)
        '''
        logger.info('''Executing query: %s 
            with account_id: %s, asset_id: %s, shares: %s
            ''', account_id, asset_id, shares
            )
        
        with self._database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (account_id, asset_id, shares))

    def account_holdings(self, account_id):
        filter_clause = 'WHERE account_id = %s'
        query = self._base_holdings_query() + filter_clause
        logger.info('Executing query: %s with account_id: %s', query, account_id)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, (account_id,))
                results = cursor.fetchall()
        
        account_holdings = [dict(lst) for lst in results]
        return account_holdings
    
    def delete_holding(self, holding_id):
        query = 'DELETE FROM holdings WHERE id = %s'
        logger.info('Executing query: %s with id: %s', query, holding_id)
        with self._database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (holding_id,))
    
    def update_holding(self, holding_id, shares):
        query = 'UPDATE holdings SET shares = %s WHERE id = %s'
        logger.info('Executing query: %s with shares: %s, id: %s', query, shares, holding_id)
        with self._database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (shares, holding_id))

    def delete_asset(self, asset_id):
        query = 'DELETE FROM assets WHERE id = %s'
        logger.info('Executing query: %s with id = %s', query, asset_id)
        with self._database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (asset_id,))
    
    def update_asset(self, asset_id, current_price):
        query = 'UPDATE assets SET current_price = %s WHERE id = %s'
        logger.info('Executing query: %s with current_price = %s and id = %s', query, current_price, asset_id)
        with self._database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (current_price, asset_id))
    
    def delete_account(self, account_id):
        query = 'DELETE FROM accounts WHERE id = %s'
        logger.info('Executing query: %s with id = %s')
        with self._database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (account_id,))
    
    def account_totals(self):
        query = f'''
            WITH base_query AS ({self._base_holdings_query()})

            SELECT 
                account_name,
                account_type,
                account_id,
                COUNT(ticker) AS number_holdings,
                SUM(market_value) AS total_market_value,
                SUM(percent) AS percent
            FROM base_query
            GROUP BY account_name, account_type, account_id
        '''
        logger.info('Executing query: %s', query)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
        
        accounts = [dict(lst) for lst in results]
        return accounts

    def asset_totals(self):
        query = f'''
            WITH base_query AS ({self._base_holdings_query()})

            SELECT 
                assets.id AS id,
                assets.ticker,
                assets.name,
                assets.category,
                MIN(assets.current_price) AS current_price,
                SUM(shares) AS total_shares,
                COUNT(DISTINCT account_id) AS accounts_holding,
                SUM(market_value) AS total_market_value,
                SUM(percent) AS percent
            FROM assets
            LEFT JOIN base_query ON assets.id = base_query.asset_id
            GROUP BY assets.id, assets.ticker, assets.name, assets.category
        '''
        logger.info('Executing query: %s', query)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
        
        assets = [dict(lst) for lst in results]
        return assets