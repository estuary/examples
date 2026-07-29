-- Cleans and types the raw orders mirror. Estuary lands numeric and timestamp
-- fields as strings in the JSON-shaped collection, so we cast them here into
-- the types the marts and quality checks expect.

with source as (
    select * from {{ source('estuary', 'orders') }}
)

select
    order_id,
    customer_name,
    cast(amount as number(10, 2))       as amount,
    lower(status)                       as status,
    cast(event_ts as timestamp_ntz)     as event_ts,
    cast(updated_at as timestamp_ntz)   as updated_at
from source
