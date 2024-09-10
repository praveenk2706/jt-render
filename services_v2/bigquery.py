import logging
import random
import time
import traceback

import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# BigQuery credentials
BIGQUERY_SERVICE_ACCOUNT_INFO = {
    "type": "service_account",
    "project_id": "property-dataset-370200",
    "private_key_id": "c6e2d46502526baa73edd6ea144395bd3c964847",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDCiKfCp50j6WM6\nLuWmhwgmCgCghYrISzJaMLrLIG/7Bki9UQdR6ARKORrYUuTuSFkPLYr3ivZ6pK9/\n+kz+I9AC7m6GsfJMEkAN9WWspwkUrk5Eort7+dYQQOiwB1q7e/c6inNUrhANExWA\nkFKMNa25pgqR83QtiRAfT2HSo5IXojUsX9whf3NtE7AoqBNe6vJ77KIudsmcFiFn\nzuW/ekSN3/RIW5mecCenQ/3lpF4e6dxkvx64xvEHlPmgTRguuk2cZJdV3Pmc/E0P\nqr2k1GFUOHH2WVTArMtr3kf/5puBqMFlPfeptU4kvYFa9wi/2mqQfGE1gStUZK9/\nJIUyDJ0/AgMBAAECggEAGWolEKc5PmHVVtqdoh3uEJop7s7DjNtWHrZTEQqR1qfB\nhQo6/JZlSRUgzrY1tnO5PuRiTWjYPrmZPA0FVgZzvI5N3kcNMqcHu0seF2pTI2KK\nUyk3eUDwmHsCa0zqq6fMatd/rcZ3zl1556vzPOIeaiypINw/h1tqwcxrKUu5YGDT\n70SWqZq4zs0igBqwMxgdL6eh/o32hCXXhOUnKzHF5CLKGO/gi1Rmy5B2d2aEL/5J\nKe89+ssbC8XeuRbuuVlvs67MDvbaqOtrwuYr9Y7FGjfuJyo1FkqjJQdBK00IJMfQ\nQpn7d/a0PY3ESU2T2YqZuUd2oxhG/t7LaQ41/s6RUQKBgQD9KtxU60uLG25EfB8m\nbQ+QwRK76Do0zsOMTcngQBWhO8inKQnkZ5p7JvZuUJzdToOCtrrJZSyMpSml1+u2\nFKNAJLx+YPIM+GOX74S8S/CCKpFogklXcz1lbLhN+abo11Bke0Xy8A510KICvm/k\n+xmYza41aGfC3vecTQCul7z4tQKBgQDEtdot3bfufhQhL+DC6Zhl+4eBvxeImYae\nG/kPS1b4qB8gwnOwdDhWsMMvT0YvWjj87LvrTTtgSrYfAt/N2fsJAkGMvCQahADq\nwX0MDzIhCCJU4xlAM+i/aAYoWiQWWywj6bjtpPTB1hxG2L/qFrltH2YQN5L6GFM7\nvn/1EeR6owKBgCjfw6T+PDsgWaQ6+fiFQS8YzQKDkuo1u8KqSH6pOhTHBzVd3df/\nliRt/PmAkGL2qyruf6fGavPmMpwxCUBjgpv3kmtBEbNgYwwDwsV36UPKxaE+78Jr\nu6zlabhFJ1qOcM9YS8nWUi9ZWN7iikRI+i/+fTLoQWBoCUg18nc9zmfFAoGBAKGu\nw00Nv3/Em3EHtFsgu4Wfb6qiUYqSfgZ5AYajqzGopiU8DvELJb+DfHrnUs3a/5pM\nTs1B5rrnzbfBmgjN0Tt/Yjd18MrmkwZh0RFXcCGcWPOi0f9mazjgHKeQJ7dEawx9\nr+WMvyyPeyB0SZHz6acatr4/n4v28HrbwrrBYSW3AoGAOp65UJI2+cFu13fIDlLd\n3oJK8kIeI+uXTZ/CfpEy0g4ECoPuzgXIb43lbin4exkfTfU5cuOdE68Lbw9AgQvU\nWoHRYoGbhlkY0PGX4cEhNEU2PRHQfyHD1lhr6KqLBmFlnMFCY+GrZxbH6APl5T1V\nCyYBKnBC3zfDHHT8aXLwQbc=\n-----END PRIVATE KEY-----\n",
    "client_email": "my-bigquery-sa@property-database-370200.iam.gserviceaccount.com",
    "client_id": "114110166144849774399",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/my-bigquery-sa%40property-database-370200.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com",
    "scopes": [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery",
    ],
}

# Configure logging
logging.basicConfig(
    filename="bigquery.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


df = pd.read_csv("./owner_id_sorted.csv", dtype=str)

# Apply this to all relevant columns
float_columns = [
    "Building_Square_Footage",
    "Lot_Acreage",
    "Lot_Area",
    "Property_Latitude",
    "Property_Longitude",
    "LMS_Sale_Price",
    "Property_Tax",
    "LMS_Mortgage_Amount",
    "ACREAGE",
    "Clay",
    "Clay loam",
    "Coarse sand",
    "Coarse sandy loam",
    "Fine sand",
    "Fine sandy loam",
    "Loam",
    "Loamy coarse sand",
    "Loamy fine sand",
    "Loamy sand",
    "Sand",
    "Sandy clay",
    "Sandy clay loam",
    "Sandy loam",
    "Silt loam",
    "Silty clay",
    "Silty clay loam",
    "Very fine sandy loam",
    "slope_mean",
    "num_buildings",
    "largest_building",
    "smallest_building",
    "total_building_area",
    "distance_to_nearest_road_from_centroid",
    "road_frontage",
    "trees_percentage",
    "built_percentage",
    "grass_percentage",
    "crops_percentage",
    "shrub_and_scrub_percentage",
    "bare_percentage",
    "water_percentage",
    "flooded_vegetation_percentage",
    "snow_and_ice_percentage",
    "Estuarine and Marine Deepwater",
    "Estuarine and Marine Wetland",
    "Freshwater Emergent Wetland",
    "Freshwater Forested/Shrub Wetland",
    "Freshwater Pond",
    "Lake",
    "Riverine",
    "parcel_area",
    "largest_rect_area",
    "percent_rectangle",
    "largest_square_area",
    "percent_square",
    "largest_rect_area_cleaned",
    "largest_square_area_cleaned",
    "percent_rectangle_area_cleaned",
    "percent_square_area_cleaned",
    "Wetlands_Total",
]

# Convert all relevant columns to float64 with error handling
for col in float_columns:
    df[col] = pd.to_numeric(df[col], errors="coerce")

    df[col] = df[col].fillna(0)

# Find all columns of dtype 'object' and not in float_columns
object_columns = df.select_dtypes(include="object").columns
columns_to_convert = [col for col in object_columns if col not in float_columns]

for col in columns_to_convert:
    df[col] = df[col].fillna("No Information")

# Convert these columns to str
df[columns_to_convert] = df[columns_to_convert].astype(str)

# Replace "/" with underscores in column names
df.columns = df.columns.str.replace(r"[\/\s]", "_", regex=True)


# Function to insert records into BigQuery with retry logic
def insert_records_to_bigquery(records, retries=1):
    for attempt in range(retries):
        try:
            logging.info(f"{records[0]} current")
            errors = client.insert_rows_json(BIGQUERY_TABLE, records)
            if errors:
                logging.error(f"Errors while inserting records: {errors}")
            else:
                logging.info(f"Successfully inserted {len(records)} records.")
            return
        except Exception as e:
            logging.error(f"Error on attempt {attempt + 1}/{retries}: {e}")
            traceback.print_exc()
            if attempt < retries - 1:
                wait_time = (2**attempt) + random.uniform(
                    0, 1
                )  # Exponential backoff with jitter
                logging.info(f"Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                raise e


BIGQUERY_TABLE = "property-database-370200.Dev_Dataset_2.Properties"
BIGQUERY_PROJECT_ID = "property-database-370200"

client = bigquery.Client(
    credentials=service_account.Credentials.from_service_account_info(
        info=BIGQUERY_SERVICE_ACCOUNT_INFO
    ),
    project=BIGQUERY_PROJECT_ID,
)


chunk_size = 1000

for i in range(68000, len(df), chunk_size):
    start = i
    end = start + chunk_size

    logging.info(f"Current chunk {start} {end}")

    chunk = df.iloc[start:end]

    records = chunk.to_dict(orient="records")

    insert_records_to_bigquery(records)
