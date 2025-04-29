import csv
import psycopg2
from db_config import DB_CONFIG
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

CSV_PATH = "./data/listings.csv"  # Make sure this path is correct

def clean_price(price_str):
    try:
        return float(price_str.replace("$", "").replace(",", ""))
    except:
        return None

def extract_and_transform():
    """
    Reads the CSV, selects relevant fields, and applies basic cleaning.
    """
    listings = []
    with open(CSV_PATH, newline='', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                listings.append({
                    "id": int(row["id"]),
                    "name": row["name"],
                    "neighbourhood": row["neighbourhood"],
                    "room_type": row["room_type"],
                    "price": clean_price(row["price"]),
                    "minimum_nights": int(row["minimum_nights"]),
                    "availability_365": int(row["availability_365"]),
                    "scraped_date": datetime.now()
                })
            except Exception as e:
                logging.warning(f"Skipping row due to error: {e}")
    return listings

def load_to_db(data):
    """
    Inserts cleaned data into the PostgreSQL database.
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS airbnb_listings (
                        id BIGINT PRIMARY KEY,
                        name TEXT,
                        neighbourhood TEXT,
                        room_type TEXT,
                        price NUMERIC,
                        minimum_nights INT,
                        availability_365 INT,
                        scraped_date TIMESTAMP
                    );
                """)
                for item in data:
                    cursor.execute("""
                        INSERT INTO airbnb_listings (id, name, neighbourhood, room_type, price, minimum_nights, availability_365, scraped_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                    """, (
                        item["id"], item["name"], item["neighbourhood"], item["room_type"],
                        item["price"], item["minimum_nights"], item["availability_365"],
                        item["scraped_date"]
                    ))
        logging.info("Data loaded successfully.")
    except Exception as e:
        logging.error(f"Database load failed: {e}")

def main():
    logging.info("Starting Airbnb ETL process...")
    data = extract_and_transform()
    logging.info(f"{len(data)} records ready to load.")
    load_to_db(data)

if __name__ == "__main__":
    main()
