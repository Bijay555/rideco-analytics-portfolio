from datetime import datetime

# Fake the Airflow context dictionary
# This simulates what Airflow passes to your function automatically
fake_kwargs = {
    'logical_date': datetime(2024, 2, 1) # Pretend it's the Feb run
}

execution_date = fake_kwargs['logical_date']
file_date = execution_date.strftime('%Y-%m')
url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{file_date}.parquet"

print(f"--- Simulation ---")
print(f"Airflow Logical Date: {execution_date}")
print(f"Generated URL: {url}")
print(f"Expected URL: ...yellow_tripdata_2024-02.parquet")
