DROP VIEW IF EXISTS views.base_holdings;
DROP SCHEMA IF EXISTS views;
DROP TABLE IF EXISTS holdings;
DROP TABLE IF EXISTS assets;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS users.users;
DROP SCHEMA IF EXISTS users;

CREATE SCHEMA IF NOT EXISTS users;
CREATE TABLE users.users (
    id serial PRIMARY KEY,
    username text UNIQUE NOT NULL,
    password_hash text NOT NULL
);

CREATE TABLE accounts (
    id serial PRIMARY KEY,
    account_name text NOT NULL,
    account_type text NOT NULL,
    user_id integer NOT NULL REFERENCES users.users(id) ON DELETE CASCADE
);

CREATE TABLE assets (
    id serial PRIMARY KEY,
    ticker text NOT NULL,
    name text NOT NULL,
    category text NOT NULL,
    current_price numeric(10,2) NOT NULL,
    user_id integer NOT NULL REFERENCES users.users(id) ON DELETE CASCADE
);

CREATE TABLE holdings (
    id serial PRIMARY KEY,
    asset_id integer NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    account_id integer NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    shares integer NOT NULL DEFAULT 0,
    user_id integer NOT NULL REFERENCES users.users(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX accounts_user_unique ON accounts (id, user_id);
CREATE UNIQUE INDEX assets_user_unique ON assets (id, user_id);

ALTER TABLE holdings
  ADD CONSTRAINT holdings_account_user_fk
  FOREIGN KEY (account_id, user_id)
  REFERENCES accounts (id, user_id)
  ON DELETE CASCADE;

ALTER TABLE holdings
  ADD CONSTRAINT holdings_asset_user_fk
  FOREIGN KEY (asset_id, user_id)
  REFERENCES assets (id, user_id)
  ON DELETE CASCADE;

CREATE INDEX accounts_user_idx ON accounts (user_id);
CREATE INDEX assets_user_idx   ON assets (user_id);
CREATE INDEX holdings_user_idx ON holdings (user_id);
CREATE INDEX holdings_account_idx ON holdings (account_id);
CREATE INDEX holdings_asset_idx   ON holdings (asset_id);

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
        holdings.id AS holding_id,
        accounts.user_id AS user_id
    FROM accounts
    LEFT JOIN holdings ON accounts.id = holdings.account_id
    LEFT JOIN assets ON assets.id = holdings.asset_id
);

ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE holdings ENABLE ROW LEVEL SECURITY;
ALTER TABLE assets ENABLE ROW LEVEL SECURITY;

CREATE POLICY accounts_owner ON accounts
USING (user_id = current_setting('app.current_user_id')::int)
WITH CHECK (user_id = current_setting('app.current_user_id')::int);

CREATE POLICY assets_owner ON assets
USING (user_id = current_setting('app.current_user_id')::int)
WITH CHECK (user_id = current_setting('app.current_user_id')::int);

CREATE POLICY holdings_owner ON holdings
USING (user_id = current_setting('app.current_user_id')::int)
WITH CHECK (user_id = current_setting('app.current_user_id')::int);

GRANT USAGE ON SCHEMA public, users, views TO app_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON accounts, holdings, assets TO app_role;
GRANT SELECT ON views.base_holdings TO app_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app_role;
