
import os
import psycopg2
import random
import time
from datetime import date, datetime, timedelta
from faker import Faker

from geo import generate_destination, update_location, is_in_delivery_distance, is_on_time

# Database connection parameters
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

# Maps to ship_status enum in the db
STATUSES = ['Processing', 'In Transit', 'At Checkpoint', 'Out For Delivery', 'Delivered', 'Delayed']

fake = Faker()


def generate_shipment_details():
    geo_loc = generate_destination()
    shipment = {
        'customer_id': random.randint(1, 1000),
        'order_id': fake.uuid4(),
        'delivery_name': fake.name(),
        'street_address': geo_loc['street_address'],
        'city': geo_loc['city'],
        'delivery_coordinates': geo_loc['delivery_coordinates'],
        'current_location': geo_loc['current_location'],
        'is_priority': random.choices([False, True], weights=[0.75, 0.25], k=1)[0]
    }
    shipment['expected_delivery_date'] = get_delivery_date(shipment['is_priority'], 
                                                           geo_loc['transit_time'])

    return shipment

def get_delivery_date(is_priority, est_transit_time):
    today = date.today()
    proc_days = random.randint(1, 2) if is_priority else random.randint(2, 5)
    ship_days = proc_days + est_transit_time
    delivery_date = today + timedelta(days=ship_days)

    return delivery_date


def insert_shipment(conn):
    print('Inserting new shipment')
    cursor = conn.cursor()
    shipment = generate_shipment_details()
    print(shipment)

    cursor.execute(
        '''INSERT INTO shipments (
            customer_id,
            order_id, 
            delivery_name, 
            street_address,
            city,
            delivery_coordinates,
            shipment_status,
            current_location,
            expected_delivery_date,
            is_priority
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);''',
        (
            shipment['customer_id'],
            shipment['order_id'], 
            shipment['delivery_name'], 
            shipment['street_address'],
            shipment['city'],
            shipment['delivery_coordinates'],
            'Processing',
            shipment['current_location'],
            shipment['expected_delivery_date'],
            shipment['is_priority']
        )
    )
    conn.commit()
    cursor.close()


def update_shipments(conn):
    print('Updating shipments')
    cursor = conn.cursor()
    cursor.execute('''SELECT id, created_at, shipment_status, current_location, 
                     delivery_coordinates, expected_delivery_date, is_priority
                   FROM shipments
                   WHERE NOT shipment_status = 'Delivered'
                   AND updated_at <= now() - interval '15 minutes';''')
    shipments = cursor.fetchall()

    for shipment in shipments:
        print(shipment)
        ship_id = shipment[0]
        created_at = shipment[1]
        status = shipment[2]
        current_location = destringify_tuple(shipment[3])
        delivery_coordinates = destringify_tuple(shipment[4])
        delivery_date = shipment[5]
        is_priority = shipment[6]

        new_status = status
        new_location = current_location
        new_delivery_date = delivery_date

        if status == 'Processing':
            min_processing_time = 0.25 if is_priority else 1.5

            if datetime.today() > (created_at + timedelta(days=min_processing_time)):
                new_status = random.choices(['Processing', 'In Transit'], 
                                        weights=[0.7, 0.3], k=1)[0]
            # else, order is still processing; simply bump updated_at
        elif status == 'Out For Delivery':
            # sometime during the day, the shipment is likely to be delivered
            new_status = random.choices(['Out For Delivery', 'Delivered'], 
                                        weights=[0.85, 0.15], k=1)[0]
            if new_status == 'Delivered':
                new_location = delivery_coordinates
        elif is_in_delivery_distance(current_location, delivery_coordinates):
            new_status = 'Out For Delivery'
        elif status == 'Delayed':
            # once you hit a delay, it's harder to get out of it
            new_status = random.choices(['Delayed', 'In Transit'], 
                                        weights=[0.8, 0.2], k=1)[0]
            
            if not is_on_time(current_location, delivery_coordinates, delivery_date):
                new_delivery_date = delivery_date + timedelta(days=1)
        else:
            # general transit case with a small chance of hitting a delay
            new_status = random.choices(['In Transit', 'At Checkpoint', 'Delayed'], 
                                        weights=[0.9, 0.09, 0.01], k=1)[0]
            if new_status != 'Delayed':
                new_location = update_location(current_location, delivery_coordinates)

            if not is_on_time(current_location, delivery_coordinates, delivery_date):
                new_delivery_date = delivery_date + timedelta(days=1)

        # all should also update updated_at so we wait on checking on them again
        cursor.execute('''UPDATE shipments
            SET shipment_status = %s,
                expected_delivery_date = %s,
                current_location = %s,
                updated_at = now()
            WHERE id = %s;
        ''', (new_status, new_delivery_date, new_location, ship_id))
        conn.commit()

    cursor.close()


def destringify_tuple(coordinate):
    return tuple(coordinate.strip('()').split(','))


def delete_old_shipments(conn):
    print('Deleting old shipments')
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id FROM shipments WHERE updated_at <= now() - interval '30 days';")
    old_shipments = cursor.fetchall()
    print(old_shipments)

    if len(old_shipments) != 0:
        cursor.execute("DELETE FROM shipments WHERE id IN %s;", (tuple(old_shipments),))
        conn.commit()

    cursor.close()


# main loop producing change events for CDC to capture
def change_loop(conn):
    while True:
        action = random.choices(['c', 'u', 'd'], weights=[0.5, 0.499, 0.001], k=1)[0]

        if action == 'c':
            insert_shipment(conn)
        elif action == 'u':
            update_shipments(conn)
        else:
            delete_old_shipments(conn)

        time.sleep(random.randint(15, 75))


def get_db_connection():
    conn_str = f"dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASSWORD}' host='{DB_HOST}' port='{DB_PORT}'"
    conn = psycopg2.connect(conn_str)
    return conn


if __name__ == "__main__":
    conn = get_db_connection()
    change_loop(conn)
    
