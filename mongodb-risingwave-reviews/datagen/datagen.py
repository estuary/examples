import os
import pymongo
import csv
import time

DB_NAME = os.getenv('MONGODB_DB', "airbnb")
DB_COLLECTION = os.getenv('MONGODB_COLLECTION', "reviews")
DB_USER = os.getenv('MONGODB_USER', "mongo")
DB_PASSWORD = os.getenv('MONGODB_PASSWORD', "mongo")
DB_HOST = os.getenv('MONGODB_HOST', "localhost")
DB_PORT = os.getenv('MONGODB_PORT', "27017")

mongo_client = pymongo.MongoClient(f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/")

mongo_db = mongo_client[f"{DB_NAME}"]
mongo_collection = mongo_db[f"{DB_COLLECTION}"]

directory = 'data'

# iterate over files in the directory
for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    # checking if it is a file
    if os.path.isfile(f):

        csvfile = open(f, 'r')
        # Read the csv file, and insert every line into MongoDB
        reader = csv.DictReader(csvfile)
        for row in reader:
            mongo_collection.insert_one(row)
            time.sleep(5)
