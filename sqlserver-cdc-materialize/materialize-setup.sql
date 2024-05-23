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

CREATE INDEX sqlserver_sales_idx on sqlserver_sales (sale_id);

CREATE OR REPLACE MATERIALIZED VIEW estuary_cnt_sales AS
SELECT customer_id, count(sale_id)
FROM sqlserver_sales
GROUP BY customer_id;

CREATE MATERIALIZED VIEW estuary_mv_total_sales_per_customer AS
SELECT customer_id, SUM(total_price) AS total_sales_amount
FROM sqlserver_sales
GROUP BY customer_id;

CREATE MATERIALIZED VIEW estuary_mv_average_unit_price_per_product AS
SELECT product_id, AVG(unit_price) AS average_unit_price
FROM sqlserver_sales
GROUP BY product_id;

CREATE MATERIALIZED VIEW estuary_mv_total_sales_quantity_per_product AS
SELECT product_id, SUM(quantity) AS total_sales_quantity
FROM sqlserver_sales
GROUP BY product_id;

CREATE MATERIALIZED VIEW estuary_mv_total_sales_per_day AS
SELECT DATE(sale_date) AS sale_day, SUM(total_price) AS total_sales_amount
FROM sqlserver_sales
GROUP BY DATE(sale_date);

CREATE MATERIALIZED VIEW estuary_mv_top_selling_products_by_quantity AS
SELECT product_id, SUM(quantity) AS total_quantity_sold
FROM sqlserver_sales
GROUP BY product_id
ORDER BY total_quantity_sold DESC
LIMIT 5;

CREATE MATERIALIZED VIEW estuary_mv_total_sales_per_month AS
SELECT date_part('year', sale_date) AS sale_year, date_part('month', sale_date) AS sale_month, SUM(total_price::float) AS total_sales_amount
FROM sqlserver_sales
GROUP BY date_part('year', sale_date), date_part('month', sale_date);
