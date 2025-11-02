{{ config(
    materialized='incremental', 
    unique_key='transaction_id',
    tags=['cx_txn']
    ) 
}}

select
    t.transaction_id,
    t.customer_id,
    t.product_id,
    d.date_id,
    t.quantity,
    t.price as unit_price,
    t.tax as tax_amount,
    (t.quantity * t.price + t.tax) as total_amount
from {{ ref('stg_customer_transactions') }} t
join {{ ref('dim_date') }} d on t.transaction_date = d.full_date