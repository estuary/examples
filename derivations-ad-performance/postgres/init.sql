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

-- Step 5: Create the tables
CREATE TABLE ad_impressions (
    impression_id SERIAL PRIMARY KEY,
    ad_id INTEGER NOT NULL,
    campaign_id INTEGER NOT NULL,
    platform VARCHAR(50) NOT NULL,
    impression_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    user_id VARCHAR(100),
    country VARCHAR(50),
    device_type VARCHAR(20)
);

-- Table for ad clicks
CREATE TABLE ad_clicks (
    click_id SERIAL PRIMARY KEY,
    impression_id INTEGER REFERENCES ad_impressions(impression_id),
    ad_id INTEGER NOT NULL,
    campaign_id INTEGER NOT NULL,
    platform VARCHAR(50) NOT NULL,
    click_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    user_id VARCHAR(100),
    landing_page_url VARCHAR(255),
    conversion_flag BOOLEAN DEFAULT FALSE
);

-- Add the tables to the publication
ALTER PUBLICATION flow_publication ADD TABLE public.ad_impressions;
ALTER PUBLICATION flow_publication ADD TABLE public.ad_clicks;

-- Step 6: Ensure replication is working under flow_capture
-- (You would typically configure a replication slot and subscription for flow_capture on the subscriber side.)
