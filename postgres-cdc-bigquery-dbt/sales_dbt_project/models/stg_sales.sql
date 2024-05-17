with source as (
    select * from {{ source('estuary', 'sales2') }}
)

select
  sale_id,
  _meta_op,
  customer_id,
  flow_published_at,
  product_id,
  quantity,
  sale_date,
  total_price,
  unit_price,
  flow_document
from source
