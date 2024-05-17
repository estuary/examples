{{
    config(
        materialized='incremental'
    )
}}

with source as (
    select * from {{ ref('stg_sales') }}
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
where _meta_op != 'd'
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  and flow_published_at >= (select coalesce(max(flow_published_at), '1900-01-01') from {{ this }})

{% endif %}