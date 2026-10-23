-- Step 1: Create the flow_capture user with replication privileges
CREATE USER IF NOT EXISTS 'flow_capture'@'%' IDENTIFIED BY 'password';

-- Step 2: Grant replication and read access to flow_capture
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'flow_capture'@'%';
GRANT SELECT ON *.* TO 'flow_capture'@'%';

-- Step 3: Create the sales table
-- (binlog format, retention and time zone are set via server flags in docker-compose.yml)
CREATE TABLE IF NOT EXISTS demo.sales (
    sale_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    sale_date DATETIME NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_price DECIMAL(10, 2) NOT NULL
);
