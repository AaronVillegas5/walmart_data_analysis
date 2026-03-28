COPY sales(store_id, store_name, date, department, revenue, transactions)
FROM '/full/path/to/Walmart.csv'
DELIMITER ','
CSV HEADER;