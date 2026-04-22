-- one row per trip
-- add a PK GUID trip_id
-- find all the duplicates and fix them
-- enrich payment_type

/* CHECK IF there ARE duplicates
-- below query returns 50, which means there are more than 50 rows that are duplicated.
SELECT *, COUNT(*)
FROM {{ ref("int_trips_unioned") }}
GROUP BY ALL
HAVING COUNT(*) > 1
LIMIT 50
*/
-- select count(1) from {{ ref("int_trips") }} -- 114,562,355
-- select count(1) from (select distinct * from {{ ref("int_trips_unioned") }} ) -- 114,827,122
-- select 'all_count', count(1) from {{ref("int_trips_unioned")}} -- 114,827,251

{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        on_schema_change='append_new_columns'
    )
}}
WITH
{% if is_incremental() %}
latest_pickup_ts AS (
    SELECT MAX(pickup_datetime) AS max_ts FROM {{ this }}
),
{% endif %}
processed_trips AS (
    SELECT
        -- trip identifiers
        tp.trip_id,
        tp.vendor_id,
        tp.taxi_type,
        tp.rate_code_id,

        -- location info
        tp.pickup_location_id,
        pz.borough AS pickup_borough,
        pz.zone AS pickup_zone,
        tp.dropoff_location_id,
        dz.borough AS dropoff_borough,
        dz.zone AS dropoff_zone,

        -- trip time
        tp.pickup_datetime,
        tp.dropoff_datetime,
        tp.store_and_fwd_flag,

        -- trip metrics
        tp.passenger_count,
        tp.trip_distance,
        tp.trip_type,
        {{ get_trip_duration_minutes('tp.pickup_datetime', 'tp.dropoff_datetime') }} as trip_duration_minutes,

        -- payment breakdown
        tp.fare_amount,
        tp.extra,
        tp.mta_tax,
        tp.tip_amount,
        tp.tolls_amount,
        tp.ehail_fee,
        tp.improvement_surcharge,
        tp.total_amount,
        tp.payment_type,
        tp.payment_type_description

    FROM {{ ref("int_trips") }} AS tp
    LEFT JOIN {{ ref("dim_locations") }} AS pz ON tp.pickup_location_id = pz.location_id
    LEFT JOIN {{ ref("dim_locations") }} AS dz ON tp.dropoff_location_id = dz.location_id

    {% if is_incremental() %}
    CROSS JOIN latest_pickup_ts AS lpts
    -- only process new trips based on pickup datetime
    WHERE tp.pickup_datetime > lpts.max_ts
    {% endif %}
)
SELECT * FROM processed_trips