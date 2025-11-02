select *
from {{ ref('stg_customer_transactions') }}
where transaction_id is not null
  and customer_id is not null
  and transaction_date is not null
  and product_id is not null
  and quantity is not null
  and price is not null
  and tax is not null
