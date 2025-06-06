
# Oracle Capture Example

This project provides a demo Oracle instance in a Docker container to test capturing Oracle data with Estuary.

## Prerequisites

To run this project, you will need to have:
* A verified Ngrok account
* Docker installed
* An [Estuary account](https://estuary.dev)

You do *not* need to have an Oracle license or Oracle Cloud account: we use a free demo instance.

## Setup

1. Build a container image of the free Oracle Database version 23.6.

    You can download Oracle's Docker images [here](https://github.com/oracle/docker-images/tree/main). Find instructions for working with Database images in [this directory](https://github.com/oracle/docker-images/tree/main/OracleDatabase/SingleInstance).

    You can, for example, make a local copy of their repo, navigate to the `/OracleDatabase/SingleInstance` path, and run:

    ```
    ./buildContainerImage.sh -v 23.6.0 -f
    ```

2. Clone this project and update the following variables:

    * `ORACLE_PWD`: Environment variable in the compose file that will be the root password for your database
    * `NGROK_AUTHTOKEN`: Environment variable in the compose file; paste in your own ngrok token to authenticate your account
    * Update the password in `init.sql` for the Estuary database user

3. Run the container and make final database configurations.

    1. Run with `docker compose up` and wait for the database to complete setup
    2. Open a bash session in the running container: `docker exec -it <your-container> bash`
    3. Start `rman`
    4. Log in as a DBA, entering your `ORACLE_PWD` when prompted: `CONNECT TARGET "sys@FREE AS SYSDBA"`
    5. Update the retention policy: `CONFIGURE RETENTION POLICY TO RECOVERY WINDOW OF 7 DAYS;`
    6. Kick off a backup: `BACKUP DATABASE PLUS ARCHIVELOG;`

4. Create a new Oracle data capture [in Estuary](https://dashboard.estuary.dev/captures) and enter:

    * **Server address:** Your ngrok endpoint (should be available from your ngrok dashboard; remove the `tcp://` protocol from the string)
    * **User:** `c##estuary_flow_user`
    * **Password:** Password for `c##estuary_flow_user` in `init.sql`
    * **Database:** `FREE`

5. Save and publish the capture. You can now view test data in an Estuary data collection, send it downstream with a materialization, or transform it with derivations.
