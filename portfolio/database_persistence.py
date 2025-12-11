import os
from contextlib import contextmanager

import logging
import psycopg2
from psycopg2.extras import DictCursor
from werkzeug.security import generate_password_hash, check_password_hash

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class DatabasePersistence:
    _initialized = False

    def __init__(self, user_id):
        if not DatabasePersistence._initialized:
            self._setup_schema()
            DatabasePersistence._initialized = True
        self.user_id = user_id
        self._BASE_HOLDINGS_QUERY = '''
            SELECT * FROM views.base_holdings 
            WHERE user_id = %s 
        '''
    
    def _setup_schema(self):
        with self._database_connect(require_user=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute('''
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = 'users' AND table_name = 'users';
                ''')
                if cursor.fetchone()[0] == 0:
                    cursor.execute('CREATE SCHEMA IF NOT EXISTS users;')
                    cursor.execute('''
                        CREATE TABLE users.users (
                            id serial PRIMARY KEY,
                            username text UNIQUE NOT NULL,
                            password_hash text NOT NULL
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
                            account_type text NOT NULL,
                            user_id integer NOT NULL REFERENCES users.users(id) ON DELETE CASCADE
                        );
                    ''')

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
                            current_price NUMERIC(10, 2) NOT NULL,
                            user_id integer NOT NULL REFERENCES users.users(id) ON DELETE CASCADE
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
                            shares integer NOT NULL DEFAULT 0,
                            user_id integer NOT NULL REFERENCES users.users(id) ON DELETE CASCADE
                        );
                    ''')
                
                cursor.execute('''
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_schema = 'views' AND table_name = 'base_holdings';
                ''')
                if cursor.fetchone()[0] == 0:
                    cursor.execute('CREATE SCHEMA IF NOT EXISTS views;')
                    cursor.execute('''
                        CREATE VIEW views.base_holdings AS (
                            SELECT
                                accounts.account_name,
                                accounts.account_type,
                                assets.ticker,
                                assets."name",
                                assets.category,
                                assets.current_price,
                                holdings.shares,
                                (assets.current_price * holdings.shares) AS market_value,
                                CASE 
                                    WHEN SUM(assets.current_price * holdings.shares) OVER () > 0 
                                    THEN (assets.current_price * holdings.shares) / SUM(assets.current_price * holdings.shares) OVER ()
                                    ELSE 0
                                END AS percent,
                                assets.id AS asset_id,
                                accounts.id AS account_id,
                                holdings.id AS holding_id,
                                accounts.user_id AS user_id
                            FROM accounts
                            LEFT JOIN holdings ON accounts.id = holdings.account_id
                            LEFT JOIN assets ON assets.id = holdings.asset_id
                        );
                    ''')

    @contextmanager
    def _database_connect(self, require_user=True):
        if os.environ.get('FLASK_ENV') == 'production':
            connection = psycopg2.connect(os.environ['DATABASE_URL'])
        else:
            connection = psycopg2.connect(dbname="portfolio")
        
        try:
            with connection:
                if require_user:
                    if self.user_id is None:
                        raise RuntimeError('user_id required for database access.')
                    with connection.cursor() as cursor:
                        set_user = 'SET LOCAL app.current_user_id = %s'
                        cursor.execute(set_user, (self.user_id,))
                yield connection
        finally:
            connection.close()
    
    def _base_holdings_query(self, filter=None):
        # Only shows accounts with holdings 
        if filter == 'has_holdings':
            filter_clause = 'AND shares IS NOT NULL '
            return self._BASE_HOLDINGS_QUERY + filter_clause

        # Shows all accounts regardless of holdings
        return self._BASE_HOLDINGS_QUERY

    def create_user(self, username, password):
        password_hash = generate_password_hash(password)
        query = 'INSERT INTO users.users (username, password_hash) VALUES (%s, %s)'
        logger.info('''Executing query: %s 
            with username: %s and password_hash: %s'''
            , query, username, password_hash)
        
        with self._database_connect(require_user=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (username, password_hash))

    def all_users(self):
        query = 'SELECT username FROM users.users'
        logger.info('Executing query: %s', query)
        with self._database_connect(require_user=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()

        return [user for (user,) in results]
    
    def load_user_credentials(self):
        query = 'SELECT * FROM users.users'
        logger.info('Executing query: %s', query)
        with self._database_connect(require_user=False) as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
        
        credentials = {row['username']: row['password_hash'] for row in rows}
        user_ids = {row['username']: row['id'] for row in rows}
        return credentials, user_ids

    def get_columns(self):
        filter_clause = 'LIMIT 0'
        query = self._base_holdings_query() + filter_clause
        logger.info('Executing query: %s with user_id: %s', query, self.user_id)
        with self._database_connect(require_user=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (self.user_id,))
                if cursor.description is None:
                    return []     
                # cursor.description is a list of tuples (name, type_code, ...)
                column_names = [column[0] for column in cursor.description]
        
        return column_names

    def all_holdings(self):
        query = self._base_holdings_query('has_holdings')
        logger.info('Executing query: %s with user_id = %s', query, self.user_id)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, (self.user_id,))
                results = cursor.fetchall()
                
        holdings = [dict(lst) for lst in results]
        return holdings

    def all_accounts(self):
        query = 'SELECT * FROM accounts WHERE user_id = %s'
        logger.info('Executing query: %s with user_id = %s', query, self.user_id)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, (self.user_id,))
                results = cursor.fetchall()

        accounts = [dict(lst) for lst in results]
        return accounts

    def all_assets(self):
        query = 'SELECT * FROM assets WHERE user_id = %s'
        logger.info('Executing query: %s with user_id = %s', query, self.user_id)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, (self.user_id,))
                results = cursor.fetchall()
            
        assets = [dict(lst) for lst in results]
        return assets
    
    def find_holding(self, holding_id):
        filter_clause = 'AND holding_id = %s'
        query = self._base_holdings_query('has_holdings') + filter_clause
        logger.info('Executing query: %s with user_id: %s, holding_id: %s', query, self.user_id, holding_id)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, (self.user_id, holding_id,))
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
            INSERT INTO accounts (account_name, account_type, user_id) 
            VALUES (%s, %s, %s)
        '''
        logger.info('''Executing query: %s 
            with account_name: %s, account_type: %s, user_id: %s
            ''', query, account_name, account_type, self.user_id
            )

        with self._database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (account_name, account_type, self.user_id))
        
    def add_asset(self, ticker, name, category, current_price):
        query = '''
            INSERT INTO assets (ticker, name, category, current_price, user_id) 
            VALUES (%s, %s, %s, %s, %s)
        '''
        logger.info('''Executing query: %s with ticker: %s, 
            name: %s, category: %s, current_price: %s, user_id: %s
            ''', ticker, name, category, current_price, self.user_id
            )
        
        with self._database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (
                    ticker, name, category, current_price, self.user_id)
                    )
    
    def add_holding(self, account_id, asset_id, shares):
        query = '''
            INSERT INTO holdings (account_id, asset_id, shares, user_id) 
            VALUES (%s, %s, %s, %s)
        '''
        logger.info('''Executing query: %s 
            with account_id: %s, asset_id: %s, shares: %s, user_id: %s
            ''', account_id, asset_id, shares, self.user_id
            )
        
        with self._database_connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (account_id, asset_id, shares, self.user_id))

    def account_holdings(self, account_id):
        filter_clause = 'AND account_id = %s'
        query = self._base_holdings_query() + filter_clause
        logger.info('Executing query: %s with user_id: %s, account_id: %s', query, self.user_id, account_id)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, (self.user_id, account_id,))
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
        logger.info('Executing query: %s with user_id: %s', query, self.user_id)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, (self.user_id,))
                results = cursor.fetchall()
                
                cursor.execute(query + 'LIMIT 0', (self.user_id,))
                if cursor.description is None:
                    return []     
                column_names = [column[0] for column in cursor.description]
        
        accounts = [dict(lst) for lst in results]
        return accounts, column_names

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
            WHERE assets.user_id = base_query.user_id
            GROUP BY assets.id, assets.ticker, assets.name, assets.category
        '''
        logger.info('Executing query: %s with user_id: %s', query, self.user_id)
        with self._database_connect() as connection:
            with connection.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, (self.user_id,))
                results = cursor.fetchall()

                cursor.execute(query + 'LIMIT 0', (self.user_id,))
                if cursor.description is None:
                    return []     
                column_names = [column[0] for column in cursor.description]
        
        assets = [dict(lst) for lst in results]
        return assets, column_names