-- Step 1: Create the flow_capture user with replication role
CREATE USER flow_capture WITH REPLICATION PASSWORD 'password';

-- Step 2: Grant read access and necessary privileges to flow_capture
GRANT pg_read_all_data TO flow_capture;
GRANT pg_write_all_data TO flow_capture;

-- Step 3: Create the flow_watermarks table and assign privileges
CREATE TABLE IF NOT EXISTS public.flow_watermarks (
    slot TEXT PRIMARY KEY,
    watermark TEXT
);

-- Grant privileges to flow_capture for the flow_watermarks table
GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO flow_capture;

-- Step 4: Create the publication and add tables
CREATE PUBLICATION flow_publication;
ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true);

-- Add flow_watermarks table to the publication
ALTER PUBLICATION flow_publication ADD TABLE public.flow_watermarks;

-- Step 5: Create the sales table
CREATE TABLE IF NOT EXISTS public.sales (
    sale_id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    sale_date TIMESTAMP NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    total_price NUMERIC(10, 2) NOT NULL
);

-- Add the sales table to the publication
ALTER PUBLICATION flow_publication ADD TABLE public.sales;

-- Step 6: Ensure replication is working under flow_capture
-- (You would typically configure a replication slot and subscription for flow_capture on the subscriber side.)
