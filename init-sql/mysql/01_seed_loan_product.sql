-- ============================================================
-- Seed los_db.loan_product: 6 static loan products (MORTGAGE, AUTO, CONSUMER, BUSINESS).
-- Referenced by loan_application.product_code.
-- ============================================================

SET NAMES utf8mb4;
USE los_db;

INSERT INTO loan_product (product_code, product_name, product_type, min_rate_pct, max_rate_pct, max_term_months, is_secured, is_active, created_at, updated_at) VALUES
    ('MORT01', 'Vay mua nhà ở', 'MORTGAGE', 7.50, 10.50, 300, 1, 1, '2024-01-01 08:00:00', '2024-01-01 08:00:00'),
    ('AUTO01', 'Vay mua ô tô', 'AUTO', 8.00, 11.00, 84, 1, 1, '2024-01-01 08:00:00', '2024-01-01 08:00:00'),
    ('CONS01', 'Vay tiêu dùng tín chấp', 'CONSUMER', 14.00, 22.00, 60, 0, 1, '2024-01-01 08:00:00', '2024-01-01 08:00:00'),
    ('CONS02', 'Vay tiêu dùng có tài sản đảm bảo', 'CONSUMER', 9.50, 13.00, 120, 1, 1, '2024-01-01 08:00:00', '2024-01-01 08:00:00'),
    ('BIZ01', 'Vay sản xuất kinh doanh', 'BUSINESS', 8.50, 12.50, 120, 1, 1, '2024-01-01 08:00:00', '2024-01-01 08:00:00'),
    ('BIZ02', 'Vay bổ sung vốn lưu động', 'BUSINESS', 9.00, 13.50, 12, 0, 1, '2024-01-01 08:00:00', '2024-01-01 08:00:00');
