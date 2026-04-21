SELECT
    -- identifiers
    CAST(vendorid AS INT) AS vendor_id,
    CAST(ratecodeid AS INT) AS rate_code_id,
    CAST(pulocationid AS INT) AS pickup_location_id,
    CAST(dolocationid AS INT) AS dropoff_location_id,

    -- timestamps
    CAST(lpep_pickup_datetime AS TIMESTAMP ) AS pickup_datetime,
    CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

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
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(ehail_fee AS NUMERIC) AS ehail_fee, -- green cab only
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    CAST(payment_type AS INT) AS payment_type,
    CAST(congestion_surcharge AS NUMERIC) AS congestion_surcharge,

    'green' AS taxi_type

FROM {{ source('raw_data', 'green_tripdata') }}

WHERE vendorid IS NOT NULL
    AND lpep_pickup_datetime IS NOT NULL
    AND lpep_dropoff_datetime IS NOT NULL
    AND pulocationid IS NOT NULL
    AND dolocationid IS NOT NULL

-- de-duplicate: if multiple trips match (same vendor, second, location, cab), keep first
QUALIFY ROW_NUMBER() OVER(
    PARTITION BY vendorid, lpep_pickup_datetime, lpep_dropoff_datetime, pulocationid, dolocationid
    ORDER BY dropoff_datetime
) = 1