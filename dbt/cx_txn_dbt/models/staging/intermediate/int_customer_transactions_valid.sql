{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='transaction_id',
        tags=['cx_txn']
    )
}}

with valid as (
  select *
  from {{ ref('base_customer_transactions') }}
)

select * from valid
  where transaction_id is not null
  and customer_id is not null
  and transaction_date is not null
  and product_id is not null
  and quantity is not null
  and price is not null
  and tax is not null
