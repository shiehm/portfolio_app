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