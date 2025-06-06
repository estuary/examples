-- set up the Estuary user and permissions; make sure to update the password
CREATE USER c##estuary_flow_user IDENTIFIED BY test123 CONTAINER=ALL;
GRANT CREATE SESSION TO c##estuary_flow_user CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##estuary_flow_user CONTAINER=ALL;
CREATE TABLE c##estuary_flow_user.FLOW_WATERMARKS(SLOT varchar(1000) PRIMARY KEY, WATERMARK varchar(4000));
GRANT INSERT, UPDATE ON c##estuary_flow_user.FLOW_WATERMARKS TO c##estuary_flow_user;
GRANT SELECT_CATALOG_ROLE TO c##estuary_flow_user CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##estuary_flow_user CONTAINER=ALL;
GRANT LOGMINING TO c##estuary_flow_user CONTAINER=ALL;
GRANT ALTER SESSION TO c##estuary_flow_user CONTAINER=ALL;
GRANT SET CONTAINER TO c##estuary_flow_user CONTAINER=ALL;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER USER c##estuary_flow_user QUOTA UNLIMITED ON USERS;

-- initialize a table and test data to check Estuary's connection
CREATE TABLE c##estuary_flow_user.inventory (uuid varchar(255) PRIMARY KEY, item_name varchar(255), price int, amount int);
INSERT INTO c##estuary_flow_user.inventory VALUES ('1234-abcd', 'Popcorn', 599, 33);
INSERT INTO c##estuary_flow_user.inventory VALUES ('5678-efgh', 'Caramel corn', 699, 17);
INSERT INTO c##estuary_flow_user.inventory VALUES ('9012-iklm', 'Cheese popcorn', 699, 26);
