{{
    config(
        materialized='table'
    )
}}

select
    vendor_id,
    pickup_at,
    dropoff_at,
    passenger_count,
    trip_distance,
    duration_min,
    payment_type,
    total_amount

from {{ ref('stg_taxi_trips') }}
