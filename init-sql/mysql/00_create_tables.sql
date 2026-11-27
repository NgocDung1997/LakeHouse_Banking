-- ============================================================
-- LOS (Loan Origination System) source tables in MySQL (database los_db).
-- Runs first on an empty mysql_data volume.
-- Links to Postgres source tables (cross-database, no FK constraint):
--   loan_application.customer_id       = crm.customer.customer_id (1..20)
--   loan_contract.customer_id          = crm.customer.customer_id (1..20)
--   loan_contract.disbursement_account = t24.account.account_num (CHAR(12))
-- ============================================================

SET NAMES utf8mb4;
USE los_db;

CREATE TABLE loan_product (
    product_code      VARCHAR(10)    NOT NULL PRIMARY KEY,          -- e.g. MORT01
    product_name      VARCHAR(100)   NOT NULL,                      -- Vietnamese name (utf8mb4)
    product_type      ENUM('MORTGAGE','AUTO','CONSUMER','BUSINESS') NOT NULL,
    min_rate_pct      DECIMAL(5,2)   NOT NULL,                      -- yearly interest, %
    max_rate_pct      DECIMAL(5,2)   NOT NULL,
    max_term_months   SMALLINT       NOT NULL,
    is_secured        TINYINT(1)     NOT NULL,                      -- 1 = collateral required (JDBC reads TINYINT(1) as boolean unless tinyInt1isBit=false)
    is_active         TINYINT(1)     NOT NULL DEFAULT 1,
    created_at        DATETIME       NOT NULL,
    updated_at        DATETIME       NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE loan_application (
    application_id    INT            NOT NULL PRIMARY KEY,
    customer_id       INT            NOT NULL,                      -- = crm.customer.customer_id (other database: no FK)
    product_code      VARCHAR(10)    NOT NULL,
    requested_amount  DECIMAL(18,2)  NOT NULL,                      -- VND
    term_months       SMALLINT       NOT NULL,
    branch_code       VARCHAR(10)    NOT NULL,
    status            ENUM('SUBMITTED','APPROVED','REJECTED','CANCELLED') NOT NULL,
    application_date  DATE           NOT NULL,                      -- event column of the incremental load
    decision_date     DATE           NULL,                          -- NULL while SUBMITTED / CANCELLED
    reject_reason     VARCHAR(200)   NULL,
    created_at        DATETIME       NOT NULL,
    updated_at        DATETIME       NOT NULL,
    CONSTRAINT fk_app_product FOREIGN KEY (product_code) REFERENCES loan_product (product_code),
    INDEX idx_app_application_date (application_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE loan_contract (
    contract_id         INT            NOT NULL PRIMARY KEY,
    application_id      INT            NOT NULL UNIQUE,
    customer_id         INT            NOT NULL,
    product_code        VARCHAR(10)    NOT NULL,
    disbursement_account CHAR(12)      NOT NULL,                    -- = t24.account.account_num of the same customer
    principal_amount    DECIMAL(18,2)  NOT NULL,
    interest_rate_pct   DECIMAL(5,2)   NOT NULL,
    term_months         SMALLINT       NOT NULL,
    start_date          DATE           NOT NULL,
    maturity_date       DATE           NOT NULL,
    outstanding_balance DECIMAL(18,2)  NOT NULL,                    -- as of 2026-11-23
    days_past_due       INT            NOT NULL DEFAULT 0,          -- DPD
    status              ENUM('ACTIVE','OVERDUE','CLOSED') NOT NULL,
    is_auto_debit       TINYINT(1)     NOT NULL,
    created_at          DATETIME       NOT NULL,
    updated_at          DATETIME       NOT NULL,
    CONSTRAINT fk_contract_app FOREIGN KEY (application_id) REFERENCES loan_application (application_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE repayment (                                           -- payments actually received (a ledger, not a schedule)
    repayment_id      BIGINT         NOT NULL PRIMARY KEY,
    contract_id       INT            NOT NULL,
    installment_no    SMALLINT       NULL,                          -- NULL = extra prepayment (no installment)
    due_date          DATE           NULL,                          -- NULL for prepayments
    payment_date      DATE           NOT NULL,                      -- event column of the incremental load
    amount_paid       DECIMAL(18,2)  NOT NULL,
    principal_paid    DECIMAL(18,2)  NOT NULL,
    interest_paid     DECIMAL(18,2)  NOT NULL,
    penalty_paid      DECIMAL(18,2)  NOT NULL DEFAULT 0,
    channel           ENUM('AUTO_DEBIT','BRANCH','MOBILE','TRANSFER') NOT NULL,
    created_at        DATETIME       NOT NULL,
    CONSTRAINT fk_repay_contract FOREIGN KEY (contract_id) REFERENCES loan_contract (contract_id),
    INDEX idx_repay_payment_date (payment_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
