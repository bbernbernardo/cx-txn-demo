{{ config(materialized='table') }}

select distinct
    product_id,
    product_name
from {{ ref('stg_customer_transactions') }}
