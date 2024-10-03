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

-- Step 7: Load pg_cron
CREATE EXTENSION pg_cron;

-- optionally, grant usage to regular users:
GRANT USAGE ON SCHEMA cron TO postgres;

CREATE TABLE wal_lsn_history (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    lsn_position pg_lsn
);

CREATE OR REPLACE FUNCTION record_current_wal_lsn()
RETURNS void AS $$
BEGIN
    INSERT INTO wal_lsn_history (lsn_position)
    VALUES (pg_current_wal_lsn());
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE VIEW wal_volume_analytics AS
WITH time_periods AS (
    SELECT
        timestamp,
        lsn_position,
        LEAD(timestamp) OVER (ORDER BY timestamp DESC) as prev_timestamp,
        LEAD(lsn_position) OVER (ORDER BY timestamp DESC) as prev_lsn
    FROM wal_lsn_history
),
calculations AS (
    SELECT
        timestamp,
        lsn_position,
        EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) as seconds_diff,
        pg_wal_lsn_diff(lsn_position, prev_lsn) as bytes_written
    FROM time_periods
    WHERE prev_lsn IS NOT NULL
)
SELECT
    timestamp,
    lsn_position,
    bytes_written as wal_bytes_since_previous,
    pg_size_pretty(bytes_written) as wal_size_since_previous,
    ROUND(bytes_written::numeric / NULLIF(seconds_diff, 0), 2) as bytes_per_second,
    pg_size_pretty((bytes_written::numeric / NULLIF(seconds_diff, 0))::bigint) || '/s' as rate_pretty,
    seconds_diff as seconds_since_previous
FROM calculations
WHERE seconds_diff > 0
ORDER BY timestamp DESC;

CREATE OR REPLACE VIEW wal_volume_summary AS
WITH time_windows AS (
    SELECT
        'Last 5 minutes' as window,
        5 as minutes,
        NOW() - INTERVAL '5 minutes' as start_time
    UNION ALL
    SELECT 'Last 15 minutes', 15, NOW() - INTERVAL '15 minutes'
    UNION ALL
    SELECT 'Last hour', 60, NOW() - INTERVAL '1 hour'
    UNION ALL
    SELECT 'Last day', 1440, NOW() - INTERVAL '1 day'
),
wal_diffs AS (
    SELECT
        h.timestamp,
        h.lsn_position,
        LAG(h.lsn_position) OVER (ORDER BY h.timestamp) as prev_lsn
    FROM wal_lsn_history h
),
calculated_wal AS (
    SELECT
        wd.timestamp,
        wd.lsn_position,
        wd.prev_lsn,
        pg_wal_lsn_diff(wd.lsn_position, wd.prev_lsn) as wal_diff
    FROM wal_diffs wd
    WHERE wd.prev_lsn IS NOT NULL
)
SELECT
    w.window,
    COUNT(*) as samples,
    pg_size_pretty(SUM(c.wal_diff)) as total_wal_size,
    pg_size_pretty(AVG(c.wal_diff)::bigint) as avg_wal_per_minute,
    pg_size_pretty((SUM(c.wal_diff)::numeric / (w.minutes * 60))::bigint) || '/s' as avg_rate
FROM
    time_windows w
    JOIN calculated_wal c ON c.timestamp > w.start_time
GROUP BY w.window, w.minutes
ORDER BY w.minutes;

SELECT cron.schedule('Record WAL LSN every minute', '*/1 * * * *', 'SELECT record_current_wal_lsn();');
