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

-- Create the artists table
CREATE TABLE artists (
    artist_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    genre VARCHAR(50),
    country VARCHAR(100),
    formed_year INTEGER,
    monthly_listeners INTEGER
);

-- Create the albums table
CREATE TABLE albums (
    album_id SERIAL PRIMARY KEY,
    artist_id INTEGER REFERENCES artists(artist_id),
    title VARCHAR(255) NOT NULL,
    release_date DATE,
    total_tracks INTEGER,
    album_type VARCHAR(50),
    label VARCHAR(100),
    total_plays INTEGER
);

-- Add the tables to the publication
ALTER PUBLICATION flow_publication ADD TABLE public.artists;
ALTER PUBLICATION flow_publication ADD TABLE public.albums;

-- Step 6: Ensure replication is working under flow_capture
-- (You would typically configure a replication slot and subscription for flow_capture on the subscriber side.)
