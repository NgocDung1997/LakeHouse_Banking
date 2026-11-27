-- t24.account: each customer has many accounts, joined via customer_id (cross-schema FK to crm)
CREATE TABLE IF NOT EXISTS t24.account (
    account_num     CHAR(12)        PRIMARY KEY,
    customer_id     INTEGER         NOT NULL REFERENCES crm.customer(customer_id),
    account_type    VARCHAR(20)     NOT NULL CHECK (account_type IN ('CHECKING','SAVINGS','CREDIT')),
    balance         NUMERIC(15,2)   NOT NULL DEFAULT 0,
    open_date       DATE            NOT NULL,
    status          VARCHAR(10)     NOT NULL CHECK (status IN ('ACTIVE','CLOSED','FROZEN')),
    last_updated    TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- 200 accounts spread evenly across the existing customers in the table
WITH cust AS (
    SELECT customer_id,
           ROW_NUMBER() OVER (ORDER BY customer_id) - 1 AS rn
    FROM crm.customer
),
cust_count AS (SELECT COUNT(*)::INT AS n FROM crm.customer)
INSERT INTO t24.account (account_num, customer_id, account_type, balance, open_date, status)
SELECT
    '1900' || LPAD(((i * 47777 + 12345678) % 100000000)::TEXT, 8, '0')                 AS account_num,
    (SELECT customer_id FROM cust WHERE rn = ((i - 1) % (SELECT n FROM cust_count)))   AS customer_id,
    (ARRAY['CHECKING','SAVINGS','CREDIT'])[floor(random()*3+1)::INT]                   AS account_type,
    ROUND((random() * 500000000)::NUMERIC, 2)                                           AS balance,
    (CURRENT_DATE - (random() * 1825)::INT)                                             AS open_date,
    (ARRAY['ACTIVE','ACTIVE','ACTIVE','ACTIVE','CLOSED','FROZEN'])[floor(random()*6+1)::INT] AS status
FROM generate_series(1, 200) AS t(i);
