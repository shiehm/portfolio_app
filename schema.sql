DROP TABLE IF EXISTS holdings;
DROP TABLE IF EXISTS assets;
DROP TABLE IF EXISTS accounts;

CREATE TABLE accounts (
    id serial PRIMARY KEY,
    account_name text NOT NULL,
    account_type text NOT NULL
);

CREATE TABLE assets (
    id serial PRIMARY KEY,
    ticker text NOT NULL,
    name text NOT NULL,
    category text NOT NULL,
    current_price numeric(10,2) NOT NULL
);

CREATE TABLE holdings (
    id serial PRIMARY KEY,
    asset_id integer NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    account_id integer NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    shares integer NOT NULL DEFAULT 0
);

DROP TABLE IF EXISTS base_holdings;
CREATE SCHEMA IF NOT EXISTS views;
CREATE VIEW views.base_holdings AS (
    SELECT
        accounts.account_name,
        accounts.account_type,
        assets.ticker,
        assets.name,
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
        holdings.id AS holding_id
    FROM accounts
    LEFT JOIN holdings ON accounts.id = holdings.account_id
    LEFT JOIN assets ON assets.id = holdings.asset_id
);

DROP TABLE IF EXISTS users;
CREATE SCHEMA IF NOT EXISTS users;
CREATE TABLE users.users (
    id serial PRIMARY KEY,
    username text UNIQUE NOT NULL,
    password_hash text NOT NULL
);