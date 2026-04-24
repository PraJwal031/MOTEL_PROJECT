import os
import boto3
import pandas as pd
from faker import Faker
from datetime import datetime

def generate_motel_data():
    fake = Faker()
    bucket = os.getenv("sfbucketpro")
    s3 = boto3.client('s3')
    
    # Unified timestamps for the daily batch
    created_now = datetime.now().isoformat()
    loaded_now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')

    # --- 1. HOSTS TABLE (10 RECORDS) ---
    hosts = []
    host_ids = []
    for _ in range(10):
        h_id = fake.unique.random_int(min=100, max=999)
        host_ids.append(h_id)
        hosts.append({
            "HOST_ID": h_id,
            "HOST_NAME": fake.name(),
            "HOST_SINCE": fake.date_this_century().isoformat(),
            "IS_SUPERHOST": fake.random_element(["true", "false"]),
            "RESPONSE_RATE": fake.random_int(min=10, max=100),
            "CREATED_AT": created_now
        })

    # --- 2. LISTINGS TABLE (99 RECORDS) ---
    listings = []
    listing_ids = []
    for _ in range(99):
        l_id = fake.unique.random_int(min=1000, max=9999)
        listing_ids.append(l_id)
        listings.append({
            "LISTING_ID": l_id,
            "HOST_ID": fake.random_element(host_ids), # Links to one of our 10 hosts
            "PROPERTY_TYPE": fake.random_element(["House", "Apartment", "Condo", "Villa"]),
            "ROOM_TYPE": fake.random_element(["Entire home", "Private room"]),
            "CITY": fake.city(),
            "COUNTRY": fake.country(),
            "ACCOMMODATES": fake.random_int(min=1, max=10),
            "BEDROOMS": fake.random_int(min=1, max=5),
            "BATHROOMS": fake.random_int(min=1, max=3),
            "PRICE_PER_NIGHT": fake.random_int(min=50, max=1000),
            "CREATED_AT": created_now,
            "LOADED_AT": loaded_now
        })

    # --- 3. BOOKINGS TABLE (99 RECORDS) ---
    bookings = []
    for _ in range(99):
        bookings.append({
            "LISTING_ID": fake.random_element(listing_ids), # Links to one of our 99 listings
            "BOOKING_DATE": fake.date_this_year().isoformat() + "T00:00:00",
            "NIGHTS_BOOKED": fake.random_int(min=1, max=21),
            "BOOKING_AMOUNT": fake.random_int(min=100, max=5000),
            "CLEANING_FEE": fake.random_int(min=20, max=200),
            "SERVICE_FEE": fake.random_int(min=10, max=100),
            "BOOKING_STATUS": fake.random_element(["confirmed", "cancelled", "stayed"]),
            "CREATED_AT": created_now
        })

    # --- UPLOAD TO S3 ---
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    tables = {"listings": listings, "hosts": hosts, "bookings": bookings}

    for name, data in tables.items():
        df = pd.DataFrame(data)
        file_key = f"raw/{name}/{name}_{timestamp}.csv"
        s3.put_object(
            Bucket=bucket,
            Key=file_key,
            Body=df.to_csv(index=False)
        )
        print(f"Uploaded {len(data)} records to {file_key}")

if __name__ == "__main__":
    generate_motel_data()