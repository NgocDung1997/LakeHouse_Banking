-- ============================================================
-- CDC Source: merchant_transaction table (Case 3 — aggregate → Iceberg)
-- Runs automatically on the first start of postgres-dungafk (Debezium CDC source in B9)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";  -- required for gen_random_uuid()

CREATE TABLE IF NOT EXISTS merchant_transaction (
    txn_id        VARCHAR(36)     PRIMARY KEY DEFAULT gen_random_uuid()::text,
    merchant_id   VARCHAR(20)     NOT NULL,
    merchant_name VARCHAR(100)    NOT NULL,
    category      VARCHAR(30)     NOT NULL,  -- FOOD_DRINK | TRANSPORT | SHOPPING | UTILITY | ENTERTAINMENT
    amount        NUMERIC(18, 2)  NOT NULL,
    txn_status    VARCHAR(10)     NOT NULL,  -- SUCCESS | FAILED | PENDING
    account_id    VARCHAR(20)     NOT NULL,
    txn_ts        TIMESTAMP       NOT NULL DEFAULT NOW(),
    created_at    TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- Seed: today's transactions so Spark has data right after startup
INSERT INTO merchant_transaction (merchant_id, merchant_name, category, amount, txn_status, account_id, txn_ts)
VALUES
    ('MER001', 'Circle K Ben Thanh',   'FOOD_DRINK',     45000,    'SUCCESS', 'ACC001', NOW() - interval '3 hours'),
    ('MER002', 'Grab Vietnam',         'TRANSPORT',      85000,    'SUCCESS', 'ACC002', NOW() - interval '2 hours 30 min'),
    ('MER003', 'Shopee',               'SHOPPING',       320000,   'SUCCESS', 'ACC001', NOW() - interval '2 hours'),
    ('MER001', 'Circle K Ben Thanh',   'FOOD_DRINK',     55000,    'SUCCESS', 'ACC003', NOW() - interval '1 hour 45 min'),
    ('MER004', 'EVN Ho Chi Minh',      'UTILITY',        450000,   'SUCCESS', 'ACC004', NOW() - interval '1 hour 30 min'),
    ('MER002', 'Grab Vietnam',         'TRANSPORT',      120000,   'SUCCESS', 'ACC005', NOW() - interval '1 hour'),
    ('MER005', 'CGV Cinemas',          'ENTERTAINMENT',  180000,   'SUCCESS', 'ACC002', NOW() - interval '45 min'),
    ('MER003', 'Shopee',               'SHOPPING',       89000,    'FAILED',  'ACC006', NOW() - interval '30 min'),
    ('MER001', 'Circle K Ben Thanh',   'FOOD_DRINK',     35000,    'SUCCESS', 'ACC007', NOW() - interval '15 min'),
    ('MER006', 'Tiki',                 'SHOPPING',       560000,   'SUCCESS', 'ACC001', NOW() - interval '5 min');
