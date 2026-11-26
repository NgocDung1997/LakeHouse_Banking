-- ============================================================
-- CDC Source: t24.core_account (Case 2 — upsert into Iceberg)
-- Runs automatically on the first start of postgres-dungafk (Debezium CDC source in B9)
-- ============================================================

CREATE TABLE IF NOT EXISTS t24.core_account (
    account_id    VARCHAR(20)     PRIMARY KEY,
    cif           VARCHAR(15)     NOT NULL,
    customer_name VARCHAR(100)    NOT NULL,
    account_type  VARCHAR(20)     NOT NULL,  -- CHECKING | SAVINGS | BUSINESS
    balance       NUMERIC(18, 2)  NOT NULL DEFAULT 0,
    status        VARCHAR(10)     NOT NULL DEFAULT 'ACTIVE', -- ACTIVE | FROZEN | CLOSED
    open_date     DATE            NOT NULL DEFAULT CURRENT_DATE,
    updated_at    TIMESTAMP       NOT NULL DEFAULT NOW()
);

INSERT INTO t24.core_account (account_id, cif, customer_name, account_type, balance, status, open_date, updated_at)
VALUES
    ('ACC001', 'CIF001', 'Nguyen Van An',   'CHECKING', 5000000.00,  'ACTIVE', '2020-01-15', NOW()),
    ('ACC002', 'CIF002', 'Tran Thi Binh',   'SAVINGS',  15000000.00, 'ACTIVE', '2019-06-20', NOW()),
    ('ACC003', 'CIF003', 'Le Van Cuong',    'CHECKING', 2500000.00,  'ACTIVE', '2021-03-10', NOW()),
    ('ACC004', 'CIF004', 'Pham Thi Dung',   'BUSINESS', 50000000.00, 'ACTIVE', '2018-11-05', NOW()),
    ('ACC005', 'CIF005', 'Hoang Van Em',    'SAVINGS',  8000000.00,  'FROZEN', '2022-07-22', NOW()),
    ('ACC006', 'CIF006', 'Vo Thi Phuong',   'CHECKING', 3200000.00,  'ACTIVE', '2023-01-08', NOW()),
    ('ACC007', 'CIF007', 'Dinh Van Quang',  'SAVINGS',  22000000.00, 'ACTIVE', '2017-04-30', NOW());
