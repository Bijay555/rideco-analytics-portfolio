with source as (
    select * from {{ source('taxi_data', 'raw_taxi_trips') }}
),

renamed as (
    select
        "VendorID" as vendor_id,
        "tpep_pickup_datetime" as pickup_at,
        "tpep_dropoff_datetime" as dropoff_at,
        "passenger_count",
        "trip_distance",
        "payment_type",
        "total_amount",

        -- Logic: Calculate duration in minutes
        EXTRACT(EPOCH FROM ("tpep_dropoff_datetime" - "tpep_pickup_datetime"))/60 as duration_min

    from source
)

select * from renamed
-- RideCo Business Logic: Filter out bad data
where trip_distance > 0
and total_amount > 0
and duration_min > 0
