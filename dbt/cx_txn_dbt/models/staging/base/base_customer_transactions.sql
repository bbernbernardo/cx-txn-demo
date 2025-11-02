{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='transaction_id',
        tags=['cx_txn']
    )
}}


with raw as (
    select * from {{ source('demo', 'raw_customer_transactions') }}
),

-- Attempt safe casting
cleaned as (
    select
        transaction_id,
        customer_id,
        product_id,
        product_name,
        quantity,
        price,
        tax,
        transaction_date,
        
        -- Validate and cast with CASE WHEN patterns
        case 
            when transaction_id ~ '^[0-9]+$' then transaction_id::int
            else null
        end as transaction_id_int,

        case 
            when customer_id ~ '^[0-9]+$' then customer_id::int
            else null
        end as customer_id_int,

        case 
            when product_id ~ '^[0-9]+$' then product_id::int
            else null
        end as product_id_int,

        case 
            when quantity ~ '^[0-9]+$' then quantity::int
            else null
        end as quantity_int,

        case 
            when price ~ '^[0-9]+(\.[0-9]+)?$' then price::float
            else null
        end as price_float,

        case 
            when tax ~ '^[0-9]+(\.[0-9]+)?$' then tax::float
            else null
        end as tax_float,

        case
            when transaction_date ~ '^\d{4}-\d{2}-\d{2}$' then transaction_date::date
        else null
end as transaction_date_casted
    from raw
)

select
    transaction_id_int as transaction_id,
    customer_id_int as customer_id,
    transaction_date_casted as transaction_date,
    product_id_int as product_id,
    product_name,
    quantity_int as quantity,
    price_float as price,
    tax_float as tax,

    -- technical fields
    'csv' as record_source,
    '{{invocation_id}}' as invocation_id,
    '{{run_started_at}}' as dbt_load_time
from cleaned
