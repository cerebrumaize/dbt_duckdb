with unioned as (
    select * from {{ ref("int_trips_unioned") }}
),

payment_type as (
    select * from {{ ref("payment_type_lookup") }}
)

select
    -- generate unique trip identifier (surrogate key pattern)
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime', 'pickup_location_id', 'dropoff_location_id', 'taxi_type']) }} AS trip_id,

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
    coalesce(u.payment_type, 0) as payment_type,
    coalesce(pt.description, 'Unknown') as payment_type_description

from unioned u
left join payment_type as pt
    on coalesce(u.payment_type, 0) = pt.payment_type
