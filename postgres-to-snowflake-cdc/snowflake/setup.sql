-- setup.sql
-- Run this in a Snowflake worksheet (as a role that can create databases,
-- roles, users, and warehouses, e.g. ACCOUNTADMIN) to create an isolated
-- environment for the pipeline: a database, a role, a user for Estuary, and a
-- dedicated extra-small warehouse to keep costs minimal.

set database_name  = 'CDC_DEMO';
set warehouse_name = 'ESTUARY_WH';
set estuary_role   = 'ESTUARY_ROLE';
set estuary_user   = 'ESTUARY_USER';

begin;

create role if not exists identifier($estuary_role);
grant role identifier($estuary_role) to role SYSADMIN;

create user if not exists identifier($estuary_user)
  default_role = $estuary_role
  default_warehouse = $warehouse_name;
grant role identifier($estuary_role) to user identifier($estuary_user);

-- An XS warehouse is plenty: continuous CDC merges are small, frequent
-- operations. auto_suspend = 60 stops the warehouse after a minute of
-- inactivity so you pay nothing between syncs; auto_resume wakes it for the
-- next merge.
create warehouse if not exists identifier($warehouse_name)
  warehouse_size = xsmall
  auto_suspend   = 60
  auto_resume    = true
  initially_suspended = true;
grant usage on warehouse identifier($warehouse_name)
  to role identifier($estuary_role);

create database if not exists identifier($database_name);
grant ownership on database identifier($database_name)
  to role identifier($estuary_role);

commit;

-- ---------------------------------------------------------------------------
-- Key-pair authentication (recommended; Snowflake is phasing out passwords for
-- programmatic access). Generate a key pair locally:
--
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM \
--     -out rsa_key.p8 -nocrypt
--   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
--
-- Then attach the public key to the user (paste the contents of rsa_key.pub
-- without the header/footer lines), and keep rsa_key.p8 for the
-- materialization config.
-- ---------------------------------------------------------------------------
-- alter user ESTUARY_USER
--   set rsa_public_key = '<contents of rsa_key.pub, without header lines>';
