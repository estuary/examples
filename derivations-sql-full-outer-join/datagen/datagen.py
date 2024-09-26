import os
import psycopg2
import time
from faker import Faker
import random

# Database connection parameters
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

fake = Faker()


# Function to generate a new artist
def generate_artist():
    name = fake.name()
    genre = random.choice(["Pop", "Rock", "Hip Hop", "Jazz", "Electronic", "Classical"])
    country = fake.country()
    formed_year = random.randint(1950, 2023)
    monthly_listeners = random.randint(1000, 10000000)
    return name, genre, country, formed_year, monthly_listeners


# Function to generate a new album
def generate_album(artist_id):
    title = fake.catch_phrase()
    release_date = fake.date_between(start_date="-50y", end_date="today")
    total_tracks = random.randint(5, 20)
    album_type = random.choice(["Studio", "Live", "Compilation", "EP"])
    label = fake.company()
    total_plays = random.randint(10000, 100000000)
    return artist_id, title, release_date, total_tracks, album_type, label, total_plays


# Function to insert a new artist into the database
def insert_artist(conn, artist):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO artists (name, genre, country, formed_year, monthly_listeners) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING artist_id",
        artist,
    )
    artist_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    return artist_id


# Function to insert a new album into the database
def insert_album(conn, album):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO albums (artist_id, title, release_date, total_tracks, album_type, label, total_plays) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        album,
    )
    conn.commit()
    cursor.close()


def main():
    conn_str = f"dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASSWORD}' host='{DB_HOST}' port='{DB_PORT}'"
    conn = psycopg2.connect(conn_str)
    print("Connected to the database!")

    try:
        while True:
            new_artist = generate_artist()
            artist_id = insert_artist(conn, new_artist)
            print("Inserted new artist:", new_artist)

            # Generate 1-5 albums for each artist
            for _ in range(random.randint(1, 5)):
                new_album = generate_album(artist_id)
                insert_album(conn, new_album)
                print("Inserted new album:", new_album)

            time.sleep(1)  # Wait for 1 second before the next operation
    except KeyboardInterrupt:
        print("Process interrupted by user.")
    finally:
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()
