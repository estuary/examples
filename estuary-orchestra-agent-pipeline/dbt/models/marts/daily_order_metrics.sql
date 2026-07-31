-- The demo's business-facing table: one row per day per status, with order
-- counts and revenue. Orchestra rebuilds this daily and then runs quality
-- checks against it before anyone downstream trusts it.

with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    cast(event_ts as date)   as order_date,
    status,
    count(*)                 as order_count,
    sum(amount)              as total_amount,
    avg(amount)              as avg_amount
from orders
group by 1, 2
order by 1, 2
