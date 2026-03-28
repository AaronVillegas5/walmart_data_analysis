CREATE TABLE sales (
    store INTEGER NOT NULL,
    date DATE NOT NULL,
    weekly_Sales NUMERIC(12,2),
    holiday_flag BOOLEAN,
    temperature NUMERIC(5,2),
    fuel_price NUMERIC(5,2),
    cpi NUMERIC(7,3),
    unemployment NUMERIC(5,3)
);