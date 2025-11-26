import streamlit as st
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

# 1. Page Config
st.set_page_config(page_title="RideCo Analytics Dashboard", layout="wide")

# 2. Load Credentials
load_dotenv()

# 3. Connect to Database (Cached for performance)
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        dbname=os.getenv('DB_NAME'),
        port=os.getenv('DB_PORT')
    )

# 4. Fetch Data using SQL
def get_data():
    conn = get_connection()
    # We query the Fact table we built with dbt
    # We aggregate by Hour of Day to see rush hour trends
    query = """
    SELECT 
        EXTRACT(HOUR FROM pickup_at) as hour_of_day,
        AVG(duration_min) as avg_duration,
        AVG(total_amount) as avg_cost,
        COUNT(*) as total_rides
    -- UPDATED: Pointing to 'dbt_dev' schema where dbt builds models
    FROM dbt_dev.fact_trips
    GROUP BY 1
    ORDER BY 1;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# 5. The Dashboard Layout
st.title("🚖 RideCo Analytics Engineering Demo")
st.markdown("### Metrics powered by dbt & Postgres")

# Load data
df = get_data()

# Top Level Metrics
col1, col2, col3 = st.columns(3)
col1.metric("Total Rides Analyzed", f"{df['total_rides'].sum():,}")
col2.metric("Avg Trip Duration", f"{df['avg_duration'].mean():.1f} min")
col3.metric("Avg Trip Cost", f"${df['avg_cost'].mean():.2f}")

st.divider()

# Charts
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("⏱️ Average Trip Duration by Hour")
    st.bar_chart(df, x='hour_of_day', y='avg_duration', color='#FF4B4B')
    st.caption("Insight: Notice how traffic peaks affect duration during rush hours.")

with col_right:
    st.subheader("💵 Average Trip Cost by Hour")
    st.line_chart(df, x='hour_of_day', y='avg_cost')
    st.caption("Insight: Higher costs might correlate with traffic or surcharges.")
