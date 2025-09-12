
import random
from datetime import date
from faker import Faker
from geopy import distance
from math import ceil

fake = Faker()

# Warehouses
# New York: (40.865173, -73.827618)
# Chicago: (41.903363, -87.668627)
# Los Angeles: (34.070080, -118.252487)
warehouse_locations = [(40.865173, -73.827618), (41.903363, -87.668627), (34.070080, -118.252487)]

# generate destination location
# estimate delivery time based on distance between closest warehouse and destination
def generate_destination():
    destination = fake.local_latlng()
    latlong = (float(destination[0]), float(destination[1]))
    street_address = fake.street_address()
    city = destination[2]

    start_location = (-85.00, 75.00)
    ship_distance = 99999

    # find the closest warehouse to the destination
    # for future, could add some randomness, like item being out of stock at a warehouse
    for location in warehouse_locations:
        warehouse_distance = distance.distance(location, latlong).miles
        if warehouse_distance < ship_distance:
            start_location = location
            ship_distance = warehouse_distance

    # estimate delivery date based on distance
    # approx. 800 mi/day
    transit_time = ceil(ship_distance / 800)

    return {
        'street_address': street_address,
        'city': city,
        'delivery_coordinates': latlong,
        'current_location': start_location,
        'transit_time': transit_time
    }

 
def update_location(current_location, delivery_coordinates):
    # if checking in about every 15 min, travel ~8 mi, trend higher
    # because we're not going in a straight line
    miles_traveled = random.randint(6, 15)

    # approximate direction
    going_north = delivery_coordinates[0] > current_location[0]
    going_east = delivery_coordinates[1] > current_location[1]

    bearing = 0
    if (going_north and going_east):
        bearing = random.randint(0, 90)
    elif (not going_north and going_east):
        bearing = random.randint(90, 180)
    elif (not going_north and not going_east):
        bearing = random.randint(180, 270)
    else:
        bearing = random.randint(270, 359)

    new_location = distance.distance(miles=miles_traveled).destination(current_location, bearing=bearing)
    new_lat = new_location.latitude
    new_long = new_location.longitude

    # add boundaries so shipment doesn't get too far away
    if new_lat < 20:
        new_lat = 20
    elif new_lat > 72:
        new_lat = 72
    if new_long < -170:
        new_long = -170
    elif new_long > -64:
        new_long = -64
    
    return (new_lat, new_long)


def is_in_delivery_distance(current_location, delivery_coordinates):
    curr_distance = distance.distance(current_location, delivery_coordinates).miles

    # if shipment is within 20 miles, it could be considered "out for delivery"
    return (curr_distance < 20)


def is_on_time(current_location, delivery_coordinates, delivery_date):
    curr_distance = distance.distance(current_location, delivery_coordinates).miles
    time_remaining = delivery_date - date.today()

    # return whether current distance falls within estimated remaining distance
    # you can travel in the remaining time
    return (time_remaining.days * 800 > curr_distance)
