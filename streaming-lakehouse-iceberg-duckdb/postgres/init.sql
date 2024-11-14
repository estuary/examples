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


CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    user_id INT,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    amount DECIMAL(10, 2)
);

CREATE TABLE transaction_metadata (
    metadata_id SERIAL PRIMARY KEY,
    transaction_id INT NOT NULL REFERENCES transactions(transaction_id) ON DELETE CASCADE,
    metadata_description TEXT NOT NULL,
    metadata_type VARCHAR(50) CHECK (metadata_type IN ('normal', 'suspicious', 'review')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


ALTER PUBLICATION flow_publication ADD TABLE public.users,public.transactions,public.transaction_metadata;

-- Add 20 fake users
INSERT INTO users (name, email)
VALUES
('John Doe', 'john@example.com'),
('Jane Smith', 'jane@example.com'),
('Michael Johnson', 'michael@example.com'),
('Emily Brown', 'emily@example.com'),
('Daniel Williams', 'daniel@example.com'),
('Sophia Jones', 'sophia@example.com'),
('Matthew Davis', 'matthew@example.com'),
('Olivia Miller', 'olivia@example.com'),
('William Wilson', 'william@example.com'),
('Ava Taylor', 'ava@example.com'),
('James Anderson', 'james@example.com'),
('Emma Martinez', 'emma@example.com'),
('Alexander Thomas', 'alexander@example.com'),
('Charlotte White', 'charlotte@example.com'),
('David Harris', 'david@example.com'),
('Isabella Martin', 'isabella@example.com'),
('Ethan Garcia', 'ethan@example.com'),
('Amelia Rodriguez', 'amelia@example.com'),
('Benjamin Lopez', 'benjamin@example.com'),
('Mia Lee', 'mia@example.com');

