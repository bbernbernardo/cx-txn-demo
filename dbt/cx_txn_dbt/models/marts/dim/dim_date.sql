{{ config(
    materialized='view',
    tags=['cx_txn']
    ) 
}}

with dates as (
    select
    -- TO DO: Create seed
        generate_series('2020-01-01'::date, '2030-12-31'::date, interval '1 day') as full_date
)
select
    to_char(full_date, 'YYYYMMDD')::int      as date_id,
    full_date,
    extract(day from full_date)              as day,
    extract(month from full_date)            as month,
    to_char(full_date, 'Month')              as month_name,
    extract(quarter from full_date)          as quarter,
    extract(year from full_date)             as year,
    extract(dow from full_date) + 1          as day_of_week,
    case when extract(dow from full_date) in (5,6) then true else false end as is_weekend,
    extract(week from full_date)             as week_of_year
from dates
