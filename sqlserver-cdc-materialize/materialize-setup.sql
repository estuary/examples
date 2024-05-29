DROP SECRET IF EXISTS estuary_refresh_token CASCADE;

 -- Grab your token at https://dashboard.estuary.dev/admin/api
CREATE SECRET estuary_refresh_token AS
  '<your-estuary-refresh-token>';

CREATE CONNECTION estuary_connection TO KAFKA (
    BROKER 'dekaf.estuary.dev',
    SECURITY PROTOCOL = 'SASL_SSL',
    SASL MECHANISMS = 'PLAIN',
    SASL USERNAME = '{}',
    SASL PASSWORD = SECRET estuary_refresh_token
);

CREATE CONNECTION csr_estuary_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'https://dekaf.estuary.dev',
    USERNAME = '{}',
    PASSWORD = SECRET estuary_refresh_token
);

CREATE SOURCE sqlserver_sales
  FROM KAFKA CONNECTION estuary_connection (TOPIC 'Dani/sqlservertest1/sales')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_estuary_connection
    ENVELOPE UPSERT;

create or replace view sales_anomalies as
  with recent_sales as (
    select * from sqlserver_sales
    where mz_now() <= sale_date + interval '7 days'
  ),
  rolling_avg as (
    select
    customer_id,
    avg(total_price) as customer_spend_avg
    from recent_sales
    group by customer_id
  )
  select
    a.customer_id, a.customer_spend_avg, s.product_id, s.quantity, s.sale_date, s.sale_id, s.total_price, s.unit_price
  from recent_sales s join rolling_avg a using(customer_id)
  where s.total_price > 1.5 * a.customer_spend_avg;

create index on sales_anomalies (customer_id);
