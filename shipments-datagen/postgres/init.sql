ALTER USER postgres REPLICATION;
GRANT pg_read_all_data TO postgres;

CREATE TABLE IF NOT EXISTS public.flow_watermarks (slot TEXT PRIMARY KEY, watermark TEXT);
GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO postgres;
CREATE PUBLICATION flow_publication;
ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true);
ALTER PUBLICATION flow_publication ADD TABLE public.flow_watermarks;

CREATE TYPE ship_status AS ENUM (
    'Processing',
    'In Transit',
    'At Checkpoint',
    'Out For Delivery',
    'Delivered',
    'Delayed'
);

CREATE TYPE coord AS (latitude FLOAT, longitude FLOAT);

CREATE TABLE shipments (
    id SERIAL PRIMARY KEY,
    customer_id INT,
    order_id UUID,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    delivery_name VARCHAR(50),
    street_address VARCHAR(255),
    city VARCHAR(50),
    delivery_coordinates coord,
    shipment_status ship_status,
    current_location coord,
    expected_delivery_date DATE,
    is_priority BOOLEAN
);

ALTER PUBLICATION flow_publication ADD TABLE public.shipments;

