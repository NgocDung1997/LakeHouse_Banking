-- customer table used to test the pipeline in phase 1
CREATE TABLE IF NOT EXISTS customer (
    customer_id     SERIAL PRIMARY KEY,
    full_name       VARCHAR(200)    NOT NULL,
    gender          CHAR(1)         NOT NULL CHECK (gender IN ('M','F')),
    date_of_birth   DATE            NOT NULL,
    phone           VARCHAR(15),
    email           VARCHAR(200),
    city            VARCHAR(100),
    customer_segment VARCHAR(20)    NOT NULL CHECK (customer_segment IN ('RETAIL','PRIORITY','VIP')),
    is_active       SMALLINT        NOT NULL DEFAULT 1,
    register_date   DATE            NOT NULL,
    last_updated    TIMESTAMP       NOT NULL DEFAULT NOW()
);

INSERT INTO customer (full_name, gender, date_of_birth, phone, email, city, customer_segment, is_active, register_date) VALUES
('Nguyễn Văn An',       'M', '1985-03-12', '0912345601', 'an.nguyen@gmail.com',       'TP.HCM',    'PRIORITY', 1, '2020-01-15'),
('Trần Thị Bình',       'F', '1990-07-24', '0987654321', 'binh.tran@gmail.com',        'Hà Nội',    'RETAIL',   1, '2021-03-20'),
('Lê Hoàng Cường',      'M', '1978-11-05', '0909123456', 'cuong.le@yahoo.com',         'Đà Nẵng',   'VIP',      1, '2019-06-10'),
('Phạm Thị Dung',       'F', '1995-02-18', '0938765432', 'dung.pham@gmail.com',        'TP.HCM',    'RETAIL',   1, '2022-05-08'),
('Hoàng Minh Đức',      'M', '1982-08-30', '0901234567', 'duc.hoang@gmail.com',        'Hải Phòng', 'PRIORITY', 1, '2020-09-14'),
('Vũ Thị Emm',          'F', '1988-04-22', '0971234567', 'emm.vu@gmail.com',           'Hà Nội',    'RETAIL',   1, '2021-11-03'),
('Đặng Quốc Phong',     'M', '1975-12-01', '0921234567', 'phong.dang@gmail.com',       'TP.HCM',    'VIP',      1, '2018-04-20'),
('Bùi Thị Giang',       'F', '1993-06-15', '0961234567', 'giang.bui@gmail.com',        'Cần Thơ',   'RETAIL',   1, '2022-08-17'),
('Ngô Thanh Hải',       'M', '1980-09-08', '0941234567', 'hai.ngo@gmail.com',          'TP.HCM',    'PRIORITY', 1, '2019-12-25'),
('Đinh Thị Hoa',        'F', '1997-01-30', '0351234567', 'hoa.dinh@gmail.com',         'Hà Nội',    'RETAIL',   1, '2023-02-14'),
('Phan Văn Khánh',      'M', '1972-05-17', '0911234567', 'khanh.phan@gmail.com',       'Đà Nẵng',   'PRIORITY', 1, '2017-07-07'),
('Lý Thị Lan',          'F', '1991-10-25', '0981234567', 'lan.ly@gmail.com',           'TP.HCM',    'RETAIL',   1, '2021-04-30'),
('Trương Hữu Mạnh',     'M', '1986-03-03', '0391234567', 'manh.truong@gmail.com',      'Hà Nội',    'VIP',      1, '2019-01-10'),
('Hồ Thị Nga',          'F', '1994-08-12', '0971234560', 'nga.ho@gmail.com',           'TP.HCM',    'RETAIL',   1, '2022-10-05'),
('Cao Xuân Phúc',       'M', '1983-11-28', '0361234567', 'phuc.cao@gmail.com',         'Hải Phòng', 'PRIORITY', 1, '2020-03-22'),
('Đỗ Thị Quỳnh',        'F', '1998-07-04', '0851234567', 'quynh.do@gmail.com',         'Hà Nội',    'RETAIL',   1, '2023-06-18'),
('Lưu Văn Sơn',         'M', '1970-02-14', '0911234560', 'son.luu@gmail.com',          'TP.HCM',    'VIP',      1, '2016-09-01'),
('Mai Thị Thu',         'F', '1989-05-20', '0981234560', 'thu.mai@gmail.com',          'Đà Nẵng',   'RETAIL',   0, '2020-12-15'),
('Nguyễn Quang Tuấn',   'M', '1992-09-09', '0901234560', 'tuan.nguyen@gmail.com',      'Cần Thơ',   'RETAIL',   1, '2021-07-28'),
('Trần Thị Uyên',       'F', '1987-12-31', '0971234561', 'uyen.tran@gmail.com',        'TP.HCM',    'PRIORITY', 1, '2019-04-11');
