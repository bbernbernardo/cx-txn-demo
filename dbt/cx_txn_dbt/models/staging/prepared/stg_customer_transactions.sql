{{ config(materialized='view') }}

select
    cast(transaction_id as integer)        as transaction_id,
    cast(customer_id as integer)           as customer_id,
    cast(transaction_date as date)         as transaction_date,
    cast(product_id as integer)            as product_id,
    trim(product_name)                     as product_name,
    cast(quantity as integer)              as quantity,
    cast(price as numeric(12,2))           as price,
    cast(tax as numeric(12,2))             as tax
from {{ ref('int_customer_transactions_valid') }}