-- setup.sql
-- Run this in a Snowflake worksheet (as a role that can create databases,
-- roles, users, and warehouses, e.g. ACCOUNTADMIN) to create the shared
-- environment used by BOTH tools in this demo:
--
--   * Estuary materializes the streamed orders into AGENT_DEMO.PUBLIC.ORDERS
--   * Orchestra runs dbt + quality tests, writing models into AGENT_DEMO.ANALYTICS
--
-- One database, one warehouse, one service user keeps the demo cheap and simple.

set database_name  = 'AGENT_DEMO';
set warehouse_name = 'AGENT_WH';
set svc_role       = 'AGENT_ROLE';
set svc_user       = 'AGENT_USER';

begin;

create role if not exists identifier($svc_role);
grant role identifier($svc_role) to role SYSADMIN;

create user if not exists identifier($svc_user)
  type = service
  default_role = $svc_role
  default_warehouse = $warehouse_name;
grant role identifier($svc_role) to user identifier($svc_user);

-- An XS warehouse is plenty: continuous CDC merges are small and frequent, and
-- the daily dbt run is light. auto_suspend = 60 stops the warehouse after a
-- minute of inactivity so you pay nothing between syncs; auto_resume wakes it.
create warehouse if not exists identifier($warehouse_name)
  warehouse_size = xsmall
  auto_suspend   = 60
  auto_resume    = true
  initially_suspended = true;
grant usage on warehouse identifier($warehouse_name)
  to role identifier($svc_role);

create database if not exists identifier($database_name);
grant ownership on database identifier($database_name)
  to role identifier($svc_role);

commit;

-- PUBLIC is where Estuary lands raw orders; ANALYTICS is where dbt builds.
--
-- Both must exist before you publish the materialization: the Snowflake
-- connector creates *tables*, not schemas, so a missing schema surfaces later as
--   Schema 'AGENT_DEMO.PUBLIC' does not exist or not authorized
-- on the connector's first commit rather than at validation time.
use role identifier($svc_role);
create schema if not exists identifier($database_name).PUBLIC;
create schema if not exists identifier($database_name).ANALYTICS;

-- ---------------------------------------------------------------------------
-- Key-pair authentication (recommended; Snowflake is phasing out passwords for
-- programmatic access). Estuary and Orchestra both authenticate as AGENT_USER.
-- Generate a key pair locally:
--
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM \
--     -out rsa_key.p8 -nocrypt
--   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
--
-- Then attach the public key to the user (paste the contents of rsa_key.pub
-- without the header/footer lines), and keep rsa_key.p8 for the connector
-- configs.
-- ---------------------------------------------------------------------------
-- alter user AGENT_USER
--   set rsa_public_key = '<contents of rsa_key.pub, without header lines>';
