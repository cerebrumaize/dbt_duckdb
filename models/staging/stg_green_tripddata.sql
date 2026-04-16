select
    -- identifiers
    CAST(vendorid AS INT) AS vendor_id,
    CAST(ratecodeid AS INT) AS rate_code_id,
    CAST(pulocationid AS INT) AS pickup_location_id,
    CAST(dolocationid AS INT) AS dropoff_location_id,

    -- timestamps
    CAST(lpep_pickup_datetime AS timestamp ) AS pickup_datetime,
    CAST(lpep_dropoff_datetime AS timestamp) AS dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    CAST(passenger_count AS INT) AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,
    -- green cab only; check data-learnings to know 1 for street-hail; ow pre-arranged
    CAST(trip_type AS INT) AS trip_type,

    -- payment info
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    cast(tolls_amount as NUMERIC) as tolls_amount,
    cast(ehail_fee as NUMERIC) as ehail_fee, -- green cab only
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    CAST(payment_type AS INT) AS payment_type,
    cast(congestion_surcharge as NUMERIC) as congestion_surcharge,

    'Green' AS taxi_type

from {{ source('raw_data', 'green_tripdata') }}

WHERE vendorid is not NULL

-- de-duplicate: if multiple trips match (same vendor, second, location, cab), keep first
qualify row_number() over(
    partition by vendorid, lpep_pickup_datetime, pulocationid, dolocationid
    order by dropoff_datetime
) = 1