-- t24.transaction: joined to account via account_num (12-digit string)
CREATE TABLE IF NOT EXISTS t24.transaction (
    transaction_id      SERIAL PRIMARY KEY,
    account_num         CHAR(12)        NOT NULL REFERENCES t24.account(account_num),
    transaction_date    TIMESTAMP       NOT NULL,
    amount              NUMERIC(15,2)   NOT NULL,
    transaction_type    VARCHAR(20)     NOT NULL CHECK (transaction_type IN ('DEPOSIT','WITHDRAWAL','TRANSFER','PAYMENT')),
    status              VARCHAR(10)     NOT NULL CHECK (status IN ('SUCCESS','FAILED','PENDING')),
    channel             VARCHAR(10)     NOT NULL CHECK (channel IN ('ATM','ONLINE','MOBILE','BRANCH')),
    description         VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_transaction_date ON t24.transaction (transaction_date);

-- 2000 transactions/day × the last 5 days (T-1 to T-5), spread evenly across accounts
WITH acc AS (
    SELECT account_num,
           ROW_NUMBER() OVER (ORDER BY account_num) - 1 AS rn
    FROM t24.account
),
acc_count AS (SELECT COUNT(*)::INT AS n FROM t24.account)
INSERT INTO t24.transaction (account_num, transaction_date, amount, transaction_type, status, channel, description)
SELECT
    (SELECT account_num FROM acc WHERE rn = ((i - 1) % (SELECT n FROM acc_count)))                AS account_num,
    (CURRENT_DATE - (day_offset || ' days')::INTERVAL) + (random() * INTERVAL '23 hours 59 minutes') AS transaction_date,
    ROUND((random() * 49990000 + 10000)::NUMERIC, 2)                                              AS amount,
    (ARRAY['DEPOSIT','WITHDRAWAL','TRANSFER','PAYMENT'])[floor(random()*4+1)::INT]                AS transaction_type,
    (ARRAY['SUCCESS','SUCCESS','SUCCESS','SUCCESS','FAILED','PENDING'])[floor(random()*6+1)::INT]  AS status,
    (ARRAY['ATM','ONLINE','MOBILE','BRANCH'])[floor(random()*4+1)::INT]                           AS channel,
    'GD-D' || day_offset || '-' || LPAD(i::TEXT, 4, '0')                                         AS description
FROM generate_series(1, 5)    AS d(day_offset),
     generate_series(1, 2000) AS t(i);
