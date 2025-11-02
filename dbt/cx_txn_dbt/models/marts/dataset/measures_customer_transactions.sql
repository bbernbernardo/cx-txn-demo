TO DO:
Total sales by month: SUM(total_amount) GROUP BY dim_date.month
Top products by revenue:	SUM(total_amount) GROUP BY dim_product.product_name
Average order size per customer:	AVG(total_amount) GROUP BY dim_customer.customer_id