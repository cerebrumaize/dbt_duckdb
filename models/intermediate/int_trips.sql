WITH unioned AS (
    SELECT *
    FROM {{ ref("int_trips_unioned") }}
    WHERE payment_type IS NOT NULL
),

payment_type AS (
    SELECT * FROM {{ ref("payment_type_lookup") }}
)

SELECT
    -- generate unique trip identifier (surrogate key pattern)
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'pickup_location_id', 'dropoff_location_id', 'taxi_type']) }} AS trip_id,

    -- Identifiers
    vendor_id,
    taxi_type,
    rate_code_id,

    -- location IDs
    u.pickup_location_id,
    u.dropoff_location_id,

    -- timestamps
    u.pickup_datetime,
    u.dropoff_datetime,

    -- trip details
    u.store_and_fwd_flag,
    u.passenger_count,
    u.trip_distance,
    u.trip_type,

    -- payment details
    u.fare_amount,
    u.extra,
    u.mta_tax,
    u.tip_amount,
    u.tolls_amount,
    u.ehail_fee,
    u.improvement_surcharge,
    u.total_amount,

    -- enrich payment type with description
    COALESCE(u.payment_type, 0) AS payment_type,
    COALESCE(pt.description, 'Unknown') as payment_type_description

FROM unioned u
LEFT JOIN payment_type AS pt
    ON COALESCE(u.payment_type, 0) = pt.payment_type

WHERE trip_distance IS NOT NULL
