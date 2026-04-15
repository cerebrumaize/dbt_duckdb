select
    -- identifiers
    cast(vendorid AS INT) as vendor_id,
    cast(ratecodeid AS INT) as rate_code_id,
    cast(pulocationid as INT) as pickup_location_id,
    cast(dolocationid as INT) as dropoff_location_id,
    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as INT) as passenger_count,
    cast(trip_distance as INT) as trip_distance,
    -- payment info
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    CAST(payment_type AS INT) AS payment_type

from {{ source("raw_data", "yellow_tripdata") }}

where vendorid is not null