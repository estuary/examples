# Estuary Hands on Lab (HoL) Workshop

## Move Data from PostgreSQL to MotherDuck

**Introduction**

In this hands-on lab, we'll be setting up a streaming CDC pipeline from PostgreSQL to MotherDuck using Estuary.  You'll use Estuary's PostgreSQL capture (source) connector and MotherDuck materialization (target) connector to set up an end-to-end CDC pipeline in three steps:
1.	You’ll capture change event data from a PostgreSQL database, using a table filled with generated realistic product data.
2.	You’ll learn how to configure Estuary to persist data as collections while maintaining data integrity.
3.	You will see how you can materialize these collections in MotherDuck to make them ready for analytics using default configuration (soft delete).
You’ll then explore ways to customize your pipelines for different use cases.
4.	You’ll create a second materialization to perform hard deletes, where a delete operation will physically delete the record from the target data warehouse.
5.	And finally, you’ll create a third materialization which will perform a slowly changing dimension type 2, where all records (inc. updates and deletes) are inserted into the target table. This is for use cases where a history/audit table is required for data warehousing. 
By the end of this tutorial, you'll have established robust and efficient data pipelines with near real-time replication of data from PostgreSQL to MotherDuck.

Before you get started, make sure to satisfy all prerequisites to complete this workshop. 

**Prerequisites**

This tutorial will assume you have access to the following resources for this hands-on lab:
- Laptop and web browser: We’ll be running the workshop from your own equipment via web-based UI.
- Docker: for convenience, we are providing a docker compose definition which will allow you to spin up a database and a fake data generator service.
- Estuary account (free tier): We’ll be creating and managing our CDC pipeline from Estuary’s UI. 
- MotherDuck account (free tier): The target data warehouse for our data pipeline is MotherDuck.  In order to follow along with the hands-on lab, a trial account is perfectly fine. You’ll also need a service token, which can be obtained from the MotherDuck console. 
- AWS S3 Bucket: An S3 bucket for staging temporary files. An S3 bucket in us-east-1 is recommended for best performance and costs, since MotherDuck is currently hosted in that region. You’ll also need your access key id and secret access key. 
- A verified Ngrok account (free tier): Estuary is a fully managed service.  Because the database used in this hands-on lab will be running on your machine, you’ll need something to expose it to the internet. ngrok is a lightweight tool that does just that.
<br>
<br>
<br>
Step 1. Set Up Source Environment Ngrok Authentication Token<br> 
Once you have created your Ngrok free account, you will need to obtain your ‘Authentication Token’ for the next step.  Save this to paste into the docker-compose.yaml script later.
<br>
<br>

PostgreSQL Setup

If you do not have PostgreSQL already running, we will set this up using a Docker image. Save the below yaml script as docker-compose.yaml.   This file will contain the service definitions for the PostgreSQL database, the mock data generator and Ngrok tunnel service (if you prefer, you can download the files from our GitHub repository here).

We will now create a init.sql script, which contains the products database table that will be used to ingest change data for streaming downstream.

Create a new folder within the root folder called schemas and paste the below SQL DDL into a file called products.sql.   This file contains the schema of the demo data generator.
	
*NOTE: This file defines the schema via a create table statement, but the actual table creation happens in the init.sql file, this is just a quirk of the Datagen data generator tool.*

For the purpose of this hands-on lab, we will be using the postgres database user.  For production use, we recommend creating a dedicated Estuary database user and assign it the privileges and grants required, per our documentation. 

We’re now ready to start the source database.  In order to initialize Postgres, the fake data generator and ngrok service, all you have to do is execute the following command from within the root folder of your hands-on lab:
> docker compose up

*NOTE: If you run into the following error:
‘postgres_cdc | psql:/docker-entrypoint-initdb.d/init.sql:8: ERROR: syntax error at or near "COMMENT" postgres_cdc | LINE 3: "name" varchar COMMENT 'faker.internet.userName()',’
Wait a few minutes and then try again, it should resolve itself.*
<br>
<br>
After a few seconds, you should see the services are up and running. The postgres_cdc service should print the following on the terminal:

postgres_cdc  | LOG:  database system is ready to accept connections

While the datagen service will be a little bit more spammy, as it prints every record it generates, but don’t be alarmed, this is enough for us to verify that both are up and running. 

Let’s see how we can connect to the database and check if we have created the objects.  In another command line tab/window execute the following command:

> docker exec -it postgres_cdc bash

Let’s connect to PostgreSQL using the following command:

> psql -h postgres_cdc -U postgres -d postgres

Enter the password postgres.
 

Before we jump into setting up the replication, you can quickly verify the data is being properly generated by connecting to the database and peeking into the products table, as shown below:

Wait a few seconds and enter the select count(*) from products; command again.  You should see a difference in values. 
<br>
<br>

Step 2. Access Estuary Dashboard

Hard part done, now comes the easy part!  
Open a web browser and go to dashboard.estuary.dev/login to access Estuary’s UI (if you haven’t already registered for a free account, please do so now to continue with this exercise). 
On the left-hand menu bar, there are several options for interacting with Estuary:
-	Welcome
-	Sources
-	Collections
-	Destinations
-	Admin

**Sources:**
Every Estuary data pipe-line starts with a capture, which is configured from ‘Sources’.   Each table adds rows of data to a corresponding Estuary collection. 
<br>**Collections:** 
The rows of data of your Estuary data pipe-lines are stored in collections: real-time data lakes of JSON documents in cloud storage (AWS S3, Azure Blob Storage or Google Cloud Storage), which can be re-played if desired instead of going back to the source.  
**Destinations:**
A materialization is how Estuary pushes data to an endpoint, binding one or more collections.  As rows of data are added to the bound collections, the materialization continuously pushes it to the destination, where it is reflected with very low latency.

### Lab Exercise 1: End-to-End Data Pipeline
Step 3. Set Up Estuary Capture

1.	Go to the sources page by clicking on the Sources on the left-hand side of your screen, then click on + New Capture.

2.	In the connector search box type in postgres and select the recommended real-time PostgreSQL connector.

3.	Let’s name this workshop for the connector and select the appropriate data-plane closest to your region (e.g. US).

4.	Complete the necessary connection details to login to PostgreSQL and click on History Mode as we’ll need this for later labs. Click on NEXT when done.

*NOTE: To obtain server address info, login to your Ngrok account and select Endpoints and Traffic Policy from the left-hand menu. This will provide the address to use. Remember to remove tcp:// when pasting the address into Estuary’s UI.*

5.	On the following screen, a few points to note:
- Schema evolution: by default enabled with the options:
         - Automatically keep schemas up to date
         - Automatically add new collections, and
         - Changing primary keys re-versions collections.
- Bindings: This will capture the tables from source. Here you can be selective if there are more tables to pick from. 
- Backfill: First time you initiate a capture task, Estuary will perform an initial load of existing data within the tables and once completed will stream changes using CDC (if the real-time connector was selected). 

NOTE: If you do not want to perform an initial load of existing data, you can change the backfill option using Backfill Mode. 

Leave everything as default settings and select NEXT again, then SAVE AND PUBLISH to deploy the connector and kick off a backfill.

NOTE: You’ll get a warning message about a watermark table not be created. You can ignore this as Estuary will create this for us. 

6.	Once your capture is set up, you’ll be able to get some insights within the Capture Details page.

NOTE: Notice the additional tabs across the top for specific information about the capture task e.g. History (audit log) and Logs (troubleshoot for errors).
7.	Go back to the Sources main page to see the capture running.


Step 4. View Associating Collection for Table Captured

8.	From the left-hand side, click on Collections

NOTE: Notice that the metrics presented here should match the metrics from the associating capture (I’ve temporarily stopped the data generator to get a consistent count across all my Estuary tasks).

9.	You can drill into each collection to view more detailed information 

NOTE: Notice that the ‘Read By’ states N/A. This will change when we set up the materialization task when we assign this collection task to it.

Step 5. Set Up Target Environment
S3 bucket and Access key ID and Secret Access Key
You can use an existing user, or create a new user for the S3 bucket and for best performance use region us-east-1. Ensure you have assigned read/write permissions (i.e. AmazonS3FullAccess) and create security credentials (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY). You can create a new user from the IAM console.

Create an Access Token within MotherDuck
You can create an access token from the Settings menu. Under Integrations you’ll find Access Token. Click on + Create token, provide a name and proceed to create token. Make a note of the token access string, which will be required within Estuary during the connection details. 

Adding Secrets to MotherDuck to Access S3
You’ll need to provide MotherDuck access to your S3 bucket. Again, you can find this within Settings > Integrations > Secrets. Click on + Add secrets and complete Name, Secret Type (Amazon S3), Access Key ID, Secret Access Key and Region (us-east-1). 

Create a New Database
Create a new database in MotherDuck for this lab exercise called lab1.


Step 6. Set Up Estuary Materialization

10.	Go to the destination page by clicking on the Destinations on the left-hand side of your screen, then click on + New Materialization.

11.	In the connector search box type in motherduck and select the recommended real-time MotherDuck connector.

12.	Let’s name this lab1 for the connector and select the appropriate data-plane closest to your region (e.g. US).

13.	Complete the necessary connection details to login to MotherDuck and enter lab1 for Bucket Path. Click on NEXT when done.

NOTE: Notice below the credential section, you have the ability to define a sync schedule for applying to the target based on time, timezone, start and end times and which days to perform sync. This is useful with data warehouses like MotherDuck to perform micro-batch applies. For this exercise, we will leave as default. 

14.	For Target Resource Naming Convention click on the drop-down and select Use Table Name Only as we want to use the main schema and not mirror the source public schema. 

15.	Scroll down to Advanced Options and click on the ADD button to select the collection to read from. In this hands-on lab you will need to select the 
<tenant name>/workshop/public/products collection. 

16.	Select NEXT again, then SAVE AND PUBLISH to deploy the connector and apply data into MotherDuck. Once your materialization is set up, you’ll be able to get some insights within the Materialization Details page.

NOTE: Notice that the metric here matches my capture and collections (I’ve temporarily stopped the data generator to get a consistent count across all my Estuary tasks).

17.	Go back to the Destination main page to see the materialization running.


Step 7. View In MotherDuck

18.	Let’s go back into MotherDuck’s dashboard and verify that data has been replicated, as shown below (I’ve temporarily stopped the data generator to get a consistent count across all my Estuary tasks):

Step 8. Some Observations About Your Newly Created Data Pipeline

Capture Side:
•	By default, Estuary will coalesce the data to only transmit the current state of the source data-point e.g. If there are 4 updates to an existing row with the same primary key, Estuary will only capture the latest update, as this mirror’s the current state, and ignores the previous 3 updates. 
•	History Mode enabled will allow you to capture all the transactions without reducing them to a final state (which we have enabled for our lab exercise). 

Materialization Side:
•	By default, Estuary will perform soft deletes downstream. When a delete operation is detected, Estuary will add an extra column on the target table indicating that the record is marked for deletion with another metadata column indicating the operation type. The record will not be physically deleted in the target. 
•	Hard Delete enabled will perform the physical deletes in the destination (we’ll look into this for lab exercise 2).
•	Delta Update (in combination with History Mode) ensures all changes are inserted as new rows in the destination, rather than overwriting existing records (we’ll look into this for lab exercise 3).
Change operation type: 'c' Create/Insert, 'u' Update, 'd' Delete.



### Lab Exercise 2: One-to-Many Topology
Step 9. Create a 2nd Materialization, Reading from the Same Collection

19.	In MotherDuck add a new database and call it lab2. 

20.	In Estuary, go to the destination page by clicking on the Destinations on the left-hand side of your screen, then click on + New Materialization.

21.	In the connector search box type in motherduck and select the recommended real-time MotherDuck connector.

22.	Let’s name this lab2 for the connector and select the appropriate data-plane closest to your region (e.g. US).

23.	Complete the necessary connection details to login to MotherDuck and ensure to select the Hard Delete checkbox this time and enter lab2 for Bucket Path. Click on NEXT when done.

24.	For Target Resource Naming Convention click on the drop-down and select Use Table Name Only as we want to use the main schema and not mirror the source public schema. 

25.	Scroll down to Advanced Options and click on the ADD button to select the collection to read from. In this hands-on lab you will need to select the 
<tenant name>/workshop/public/products collection. 

26.	Under Advanced Options > Config > Field Selection scroll down to column _meta/op and click EXCLUDE to remove this column from selection as we don’t require this for our purpose in this lab exercise. 

27.	Select NEXT again, then SAVE AND PUBLISH to deploy the connector and apply data into MotherDuck. Once your materialization is set up, go back into MotherDuck’s dashboard and verify that data has been replicated.

 

### Lab Exercise 3: Slowly Changing Dimension Type 2
Step 10. Create a 3rd Materialization to Insert All Records

Prerequisite for this is to enable History Mode on capture side, which we already did in lab exercise 1. 

You’ll also need to configure the products table to use the entire row as the unique identifier for logical replication to support SCD2. Let’s go back into PSQL and perform an ALTER TABLE.

ALTER TABLE products REPLICA IDENTITY FULL;

Now let’s set up our 3rd materialization task.

28.	In MotherDuck add a new database and call it lab3. 

29.	In Estuary. go to the destination page by clicking on the Destinations on the left-hand side of your screen, then click on + New Materialization.

30.	In the connector search box type in motherduck and select the recommended real-time MotherDuck connector.

31.	Let’s name this lab3 for the connector and select the appropriate data-plane closest to your region (e.g. US).

32.	Complete the necessary connection details to login to MotherDuck and enter lab3 for Bucket Path. Click on NEXT when done.

33.	For Target Resource Naming Convention click on the drop-down and select Use Table Name Only as we want to use the main schema. 

34.	Scroll down to Advanced Options and click on the ADD button to select the collection to read from. In this hands-on lab you will need to select the 
<tenant name>/workshop/public/products collection. 

35.	Under Advanced Options > Config > Resource Configuration ensure to select the Delta Update checkbox this time. Click on NEXT when done.

36.	Select NEXT again, then SAVE AND PUBLISH to deploy the connector and apply data into MotherDuck. Once your materialization is set up, go back into MotherDuck’s dashboard and verify that data has been replicated.


Step 11. UPDATE and DELETE a Record

37.	At this point you will need to stop the datagen container (if you already haven’t done so). You can do this via command line:

> docker stop datagen

Or you can do this via docker UI:
 
38.	Let’s go back into the PostgreSQL command line (page 6 for log in instructions if you’ve closed this window).

39.	Find a record to change within the source PostgreSQL database. I am going to use record ID 17 as this is the first record I see within my table

> SELECT * FROM products 
   ORDER BY id ASC 
   LIMIT 1;

 

40.	Perform an UPDATE on this record:

> UPDATE products 
   SET name = 'SmallData'
   WHERE id = 17;

 
41.	Perform a DELETE on this record:

> DELETE FROM products 
   WHERE id = 17;

 











