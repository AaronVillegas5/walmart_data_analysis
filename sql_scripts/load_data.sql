COPY sales(store, date, weekly_Sales, holiday_flag, temperature, fuel_price, cpi, unemployment)
FROM '/full/path/to/Walmart.csv'
DELIMITER ','
CSV HEADER;