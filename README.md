# rideco-analytics-portfolio

🚖 RIDECO Taxi Analytics Engineering Pipeline

"Building the data foundation that powers smarter transit solutions."

📖 Project Overview

This project simulates a production-grade ELT (Extract, Load, Transform) pipeline for a ride-sharing company similar to RideCo.

The goal was not just to visualize data, but to build a robust Analytics Engineering architecture that ensures data accuracy, modularity, and performance. I ingested millions of rows of NYC Taxi data, modeled it using a Star Schema, and built an interactive dashboard for stakeholders.

🏗️ The Architecture
```

graph LR
    A[NYC TLC API] -->|Extract (Python/Pandas)| B(Airflow DAG)
    B -->|Load (Append)| C[(PostgreSQL Raw)]
    C -->|Transform (dbt)| D{dbt Staging View}
    D -->|Materialize| E[dbt Fact Table]
    E -->|Viz| F[Streamlit Dashboard]
    
    subgraph Quality Gates
    D -- Tests --> G[dbt test]
    end
```

🚀 Key Features & Engineering Decisions

1. Robust Data Ingestion (Airflow & Python)

Instead of manual CSV uploads, I built an automated pipeline using Apache Airflow.

Idempotency: The pipeline accepts a logical_date parameter. If I need to backfill data for January 2024, I can trigger that specific run date, and the script dynamically generates the correct URL.

Memory Management: Utilized pandas chunking to handle large Parquet files without crashing memory.

Security: All database credentials are managed via environment variables (.env), ensuring no secrets are hardcoded.

2. Analytics Engineering (dbt)

Transformation logic is decoupled from the visualization layer using dbt (data build tool).

Staging Layer (Views): Cleans raw column names (e.g., tpep_pickup_datetime → pickup_at) and handles type casting.

Marts Layer (Tables): Materialized as physical tables for high-performance querying in the dashboard.

Business Logic: engineered metrics like duration_min and filtered out data anomalies (e.g., negative fares or 0-mile trips).

3. Data Quality & Governance

Trust is the currency of data. I implemented automated testing to catch bugs before they reach the dashboard:

Schema Tests: Enforced unique and not_null constraints on Primary Keys.

Business Tests: Validated that dropoff_time > pickup_time and passenger_count > 0.

4. Code-First Visualization (Streamlit)

Instead of a static report, I built a dynamic Python web app.

Insight: Analyze peak rush hours to optimize fleet allocation.

Performance: Queries the pre-aggregated fact_trips table for sub-second load times.

📂 Project Structure
```

rideco-analytics-portfolio/
├── dags/                  # Airflow DAGs for orchestration
│   └── rideco_pipeline.py # The Extract & Load Logic
├── dbt_rideco/            # The Transformation Layer
│   ├── models/
│   │   ├── staging/       # Cleaning Views
│   │   └── marts/         # Final Fact Tables
│   └── tests/             # Data Quality Checks
├── scripts/               # Helper Python scripts
│   └── ingest_data.py     # Standalone ingestion script
├── dashboard.py           # Streamlit Visualization App
└── requirements.txt       # Python dependencies
```

🛠️ How to Run This Project

Prerequisites

Ubuntu / Linux / Mac (or WSL for Windows)

Python 3.8+

PostgreSQL

1. Setup Environment

Clone the repo and install dependencies:
```

git clone [https://github.com/Bijay555/rideco-analytics-portfolio.git](https://github.com/Bijay555/rideco-analytics-portfolio.git)
cd rideco-analytics-portfolio
pip install -r requirements.txt
```

2. Configure Credentials

Create a .env file in the root directory:
```
DB_USER=username 
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=database_name
```
3. Run the Pipeline

You can run the ingestion script manually or trigger the Airflow DAG:
```
python3 scripts/ingest_data.py
```

4. Transform Data

Run the dbt models and tests:
```
cd dbt_rideco
dbt run   # Builds the tables
dbt test  # Validates data quality

```
5. Launch Dashboard

Start the local web server:
```
cd ..
streamlit run dashboard.py

```
📊 Business Insights


![RideCo dashboard Image](https://github.com/Bijay555/rideco-analytics-portfolio/blob/main/images/dashboard.png)

Through this analysis, I identified:

Peak Congestion: Average trip duration spikes by 40% between 5 PM and 6 PM on weekdays.

Revenue Drivers: Trips to/from JFK Airport yield the highest average fare but have high variability in duration.


