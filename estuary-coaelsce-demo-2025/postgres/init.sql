-- Ensure replication and data read access
ALTER USER postgres REPLICATION;
GRANT pg_read_all_data TO postgres;

-- Watermarks table for replication tracking
CREATE TABLE IF NOT EXISTS public.flow_watermarks (
    slot TEXT PRIMARY KEY,
    watermark TEXT
),
GRANT ALL PRIVILEGES ON TABLE public.flow_watermarks TO postgres;

-- Create a publication for logical replication
CREATE PUBLICATION flow_publication;
ALTER PUBLICATION flow_publication SET (publish_via_partition_root = true),
ALTER PUBLICATION flow_publication ADD TABLE public.flow_watermarks;

-- Products table (Static list of 30 pet store products)
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR,
    category VARCHAR,
    price NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
),

-- Transactions table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    user_id INT,
    product_id INT,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    amount NUMERIC,
    payment_method TEXT
),

-- Reviews table
CREATE TABLE IF NOT EXISTS reviews (
    review_id SERIAL PRIMARY KEY,
    user_id INT,
    product_id INT,
    rating INT,
    review_text TEXT,
    review_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
),

-- Add the new tables to the publication
ALTER PUBLICATION flow_publication ADD TABLE public.transactions, public.products, public.reviews;

-- Insert 30 pet store products
INSERT INTO products (name, category, price)
VALUES
('Organic Dog Food', 'Dog Supplies', 29.99),
('Cat Scratching Post', 'Cat Supplies', 39.99),
('Chew Toy for Dogs', 'Dog Supplies', 14.99),
('Luxury Cat Bed', 'Cat Supplies', 49.99),
('Stainless Steel Dog Bowl', 'Dog Supplies', 12.99),
('Interactive Laser Toy for Cats', 'Cat Supplies', 24.99),
('Large Dog Crate', 'Dog Supplies', 79.99),
('Cat Litter Box', 'Cat Supplies', 34.99),
('Dog Training Pads', 'Dog Supplies', 22.99),
('Automatic Pet Feeder', 'Dog Supplies', 89.99),
('Waterproof Dog Jacket', 'Dog Supplies', 27.99),
('Catnip Toys Pack', 'Cat Supplies', 9.99),
('Pet Grooming Kit', 'Dog Supplies', 19.99),
('Soft Plush Cat Cave', 'Cat Supplies', 44.99),
('LED Collar for Dogs', 'Dog Supplies', 18.99),
('Adjustable Cat Harness', 'Cat Supplies', 15.99),
('Portable Dog Water Bottle', 'Dog Supplies', 16.99),
('Self-Cleaning Litter Box', 'Cat Supplies', 129.99),
('Squeaky Bone Toy', 'Dog Supplies', 9.49),
('Cat Window Perch', 'Cat Supplies', 28.99),
('Large Dog Bed', 'Dog Supplies', 59.99),
('Canned Wet Cat Food (Pack of 12)', 'Cat Supplies', 24.99),
('Rubber Ball for Aggressive Chewers', 'Dog Supplies', 12.49),
('Interactive Cat Puzzle Toy', 'Cat Supplies', 19.99),
('Dog Flea & Tick Collar', 'Dog Supplies', 22.99),
('Clumping Cat Litter (40lb)', 'Cat Supplies', 18.99),
('Dog Training Clicker', 'Dog Supplies', 7.99),
('Heated Cat Bed', 'Cat Supplies', 42.99),
('Pet First Aid Kit', 'Dog Supplies', 25.99),
('Cat Tunnel Play Toy', 'Cat Supplies', 29.99),
('Dog Toothbrush & Toothpaste Set', 'Dog Supplies', 14.99),
('Cat Water Fountain', 'Cat Supplies', 34.99),
('Dog Poop Bags (600 Count)', 'Dog Supplies', 14.99),
('Cat Scratcher Lounge', 'Cat Supplies', 19.99),
('Dog Nail Clippers', 'Dog Supplies', 9.99),
('Cat Carrier Backpack', 'Cat Supplies', 39.99),
('Dog Cooling Mat', 'Dog Supplies', 22.99),
('Cat Treats Variety Pack', 'Cat Supplies', 12.99),
('Dog Seat Belt', 'Dog Supplies', 9.99),
('Cat Grass Growing Kit', 'Cat Supplies', 14.99),
('Dog Brush & Comb Set', 'Dog Supplies', 11.99),
('Cat Collar with Bell', 'Cat Supplies', 7.99),
('Dog Squeaky Toy Pack', 'Dog Supplies', 19.99),
('Cat Grooming Glove', 'Cat Supplies', 8.99),
('Dog Poop Scooper', 'Dog Supplies', 16.99),
('Cat Food & Water Bowl Set', 'Cat Supplies', 12.99),
('Dog Treat Pouch', 'Dog Supplies', 8.99),
('Cat Nail Clippers', 'Cat Supplies', 6.99),
('Dog Frisbee Toy', 'Dog Supplies', 9.99),
('Cat Dental Treats (Pack of 30)', 'Cat Supplies', 14.99),
('Dog Bandana Set', 'Dog Supplies', 11.99)
