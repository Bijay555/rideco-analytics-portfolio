from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# This forces Python to look for .env in the parent directory of the script
script_path = Path(__file__).parent
env_path = script_path.parent / '.env'
load_dotenv(dotenv_path=env_path)


def ingest_dynamic_data(**kwargs):
    # 1. GET THE LOGICAL DATE
    # Airflow passes the 'logical_date' (the date the data covers) in kwargs.
    # If the job runs on Feb 2nd, the logical_date is Jan 1st (Monthly schedule).
    execution_date = kwargs['logical_date']
    
    # 2. FORMAT THE URL
    # We turn the date object into a string "2024-01"
    file_date = execution_date.strftime('%Y-%m')
    
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{file_date}.parquet"
    
    print(f"--- RIDECO PIPELINE ---")
    print(f"Processing Data For: {file_date}")
    print(f"Fetching URL: {url}")
    
    # 3. DATABASE CONNECTION
    USER = os.getenv('DB_USER')
    PASSWORD = os.getenv('DB_PASSWORD')
    HOST = os.getenv('DB_HOST')
    DB_NAME = os.getenv('DB_NAME')
    
    # Check for missing config
    if not USER:
        raise ValueError("Environment variables not loaded. Check .env path.")

    conn_str = f'postgresql://{USER}:{PASSWORD}@{HOST}:5432/{DB_NAME}'
    engine = create_engine(conn_str)
    
    # 4. DOWNLOAD & LOAD
    try:
        # Check if file exists (NYC data is sometimes late)
        df = pd.read_parquet(url)
        
        df.to_sql(
            name='raw_taxi_trips', 
            con=engine, 
            if_exists='append', # Append monthly data, don't delete history!
            index=False, 
            chunksize=10000
        )
        print(f"Successfully loaded {len(df)} rows for {file_date}.")
        
    except Exception as e:
        print(f"Error fetching data for {file_date}. The file might not be published yet.")
        raise e

# DEFINE THE DAG
with DAG(
    dag_id='rideco_monthly_ingest',
    default_args={
        'owner': 'bijay',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    # "0 0 2 * *" = At 00:00 on day-of-month 2.
    schedule_interval='0 0 2 * *', 
    start_date=datetime(2024, 1, 1),
    catchup=False # Set to True if you want to download ALL months since Jan 2024 right now
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_taxi_data',
        python_callable=ingest_dynamic_data,
        provide_context=True # CRITICAL: This allows us to access logical_date
    )
```


