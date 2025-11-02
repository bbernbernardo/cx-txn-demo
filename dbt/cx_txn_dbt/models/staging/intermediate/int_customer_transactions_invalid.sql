select *
from {{ ref('stg_customer_transactions') }}
where transaction_id is null
   or customer_id is null
   or transaction_date is null
   or product_id is null
   or quantity is null
   or price is null
   or tax is null
