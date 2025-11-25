import pandas as pd
from sqlalchemy import create_engine
from time import time
import os
from dotenv import load_dotenv
from pathlib import Path  # <--- Add this

def ingest_data():
    # 1. Load Environment Variables
    # This forces Python to look for .env in the parent directory of the script
    script_path = Path(__file__).parent
    env_path = script_path.parent / '.env'
    load_dotenv(dotenv_path=env_path)

    # Fetch credentials from the environment (returns None if not found)
    USER = os.getenv('DB_USER')
    PASSWORD = os.getenv('DB_PASSWORD')
    HOST = os.getenv('DB_HOST')
    PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    TABLE_NAME = 'raw_taxi_trips'
    
    # 2. The Data Source
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-09.parquet"

    # 3. Create Database Connection
    # We add a check to ensure variables loaded correctly
    if not all([USER, PASSWORD, HOST, PORT, DB_NAME]):
        print("CRITICAL ERROR: Missing environment variables. Did you create the .env file?")
        return

    connection_string = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}'
    engine = create_engine(connection_string)

    print(f"--- Starting Ingestion Job ---")
    print(f"Connecting to database: {DB_NAME} on {HOST}")
    print(f"Downloading data from: {url}")
    
    try:
        # 4. Download and Read Data
        t_start = time()
        df = pd.read_parquet(url)
        t_end = time()
        print(f"Download complete. Loaded {len(df)} rows in {t_end - t_start:.2f} seconds.")

        # 5. Load Data into PostgreSQL
        print(f"Writing data to table '{TABLE_NAME}'...")
        
        df.to_sql(name=TABLE_NAME, con=engine, if_exists='replace', index=False, chunksize=10000)
        
        print("Success! Data ingestion finished.")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        print("Tip: Check if your Postgres server is running (sudo service postgresql status)")

if __name__ == '__main__':
    ingest_data()
