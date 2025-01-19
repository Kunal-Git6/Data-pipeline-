#import libraries
import requests
import pandas as pd
import sqlite3
import logging
from config import API_URL, PARAMS, DB_PATH, LOG_FILE

#setup logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

#define extract_data

def extract_data(api_url, params):
    logging.info("Extracting data from the API...")
    response = requests.get(api_url, params=params)
    response.raise_for_status()
    return response.json()

#define transform_data

def transform_data(data):
    logging.info("Transforming data...")
    df = pd.DataFrame(data)
    df = df[["id", "symbol", "name", "current_price", "market_cap", "total_volume"]]
    df.columns = ["ID", "Symbol", "Name", "Current Price (USD)", "Market Cap", "Total Volume"]
    return df

#define load_data

def load_data(df, db_path):
    logging.info("Loading data into the database...")
    conn = sqlite3.connect(db_path)
    df.to_sql("crypto_prices", conn, if_exists="replace", index=False)
    conn.close()

#define main

def main():
    logging.info("Pipeline started.")
    try:
        data = extract_data(API_URL, PARAMS)
        df = transform_data(data)
        load_data(df, DB_PATH)
        logging.info("Pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    main()

