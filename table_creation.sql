CREATE TABLE orders (
    order_date DATE,
    expiry_date DATE,
    product_name VARCHAR2(100),
    quantity NUMBER,
    unit_price NUMBER,
    channel VARCHAR2(50),
    payment_method VARCHAR2(50),
    discount NUMBER,
    total_price NUMBER
);