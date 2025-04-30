# Shipments Datagen

This app generates fake shipments data and stores it in a PostgreSQL database for future ingestion in StarTree.

## Prerequisites

* Docker
* Ngrok
* An Estuary account

## Quick instructions

1. Add your secrets to the `docker-compose.yml` file. This includes adding your ngrok auth token and your desired Postgres password.

2. From the `shipments-datagen` directory, run: `docker-compose up -d`

3. Go to the **Endpoints** tab in your ngrok dashboard to find the public endpoint associated with your newly-created Postgres database.

4. Enter the ngrok host URL and your Postgres details into [Estuary's Postgres capture connector](https://docs.estuary.dev/reference/Connectors/capture-connectors/PostgreSQL/).

5. [Materialize to any supported connector.](https://docs.estuary.dev/reference/Connectors/materialization-connectors/)

We'll be materializing to StarTree to create a spiffy real-time dashboard. While the frontend is in the works, this code can be used to easily set up a data-generating backend to test pipeline setup and CDC.

## The data

Generated data is currently associated with a single `shipments` table that consists of:

| Field name | Data type | Description |
| --- | --- | --- |
| `id` | integer | Serial primary key for the table |
| `customer_id` | integer | Randomly generated; indicates foreign key for a fictional `customer` table |
| `order_id` | UUID | Randomly generated universally unique identifier for the order |
| `created_at` | timestamp | Date-time when the order was generated |
| `updated_at` | timestamp | Date-time when the order was last modified |
| `delivery_name` | string | Randomly generated name of a person receiving the delivery |
| `street_address` | string | Randomly generated street address |
| `city` | string | Randomly generated city name |
| `delivery_coordinates` | tuple containing two float values | Randomly generated point within the US |
| `shipment_status` | string/enum | One of: 'Processing', 'In Transit', 'At Checkpoint', 'Out For Delivery', 'Delivered', 'Delayed', depending on current point in the shipment process |
| `current_location` | tuple containing two float values | Shipment's current coordinates; will be somewhere between a set warehouse location and the delivery coordinates |
| `expected_delivery_date` | date | Approximate expected delivery, based on distance and shipment priority |
| `is_priority` | boolean | Whether or not a shipment is considered priority; affects initial processing time |

New shipments are generated approximately every minute. Existing shipments are updated approximately every 15 minutes and progress through shipment statuses while updating their current locations. While initial and ending coordinates should be actual points within the United States, the route between the two is randomly generated rather than corresponding to actual roads.

The data is meant to be used for demonstration purposes, providing a facsimile of real-time shipping and logistics data.
