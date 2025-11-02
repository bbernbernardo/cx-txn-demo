{{ config(
    materialized='view',
    tags=['cx_txn']
    ) 
}}

select distinct
    customer_id
    -- Add attributes when available (e.g. name, region)
from {{ ref('stg_customer_transactions') }}
