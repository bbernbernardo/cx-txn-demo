COPY demo.raw_customer_transactions(transaction_id,customer_id,transaction_date,product_id,product_name,quantity,price,tax)
FROM '../data/customer_transactions.csv'
DELIMITER ','
CSV HEADER;