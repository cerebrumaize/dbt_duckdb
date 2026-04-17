-- one row per trip
-- add a PK GUID trip_id
-- find all the duplicates and fix them
-- enrich payment_type

/* check if there are duplicates
-- below query returns 50, which means there are more than 50 rows that are duplicated.
select *, count(*)
from {{ ref("int_trips_unioned") }}
group by all
having count(*) > 1
limit 50
*/
-- select count(1) from {{ ref("int_trips") }} -- 114,562,355
-- select count(1) from (select distinct * from {{ ref("int_trips_unioned") }} ) -- 114,827,122
-- select 'all_count', count(1) from {{ref("int_trips_unioned")}} -- 114,827,251

{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}
with 
{% if is_incremental() %}
latest_pickup_ts as (
    select max(pickup_datetime) as max_ts from {{ this }}
),
{% endif %}
processed_trips as (
    select
        -- trip identifiers
        tp.trip_id,
        tp.vendor_id,
        tp.taxi_type,
        tp.rate_code_id,

        -- location info
        tp.pickup_location_id,
        pz.borough as pickup_borough,
        pz.zone as pickup_zone,
        tp.dropoff_location_id,
        dz.borough as dropoff_borough,
        dz.zone as dropoff_zone,

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

    from {{ ref('int_trips') }} as tp
    left join {{ ref('dim_locations') }} as pz on tp.pickup_location_id = pz.location_id
    left join {{ ref('dim_locations') }} as dz on tp.dropoff_location_id = dz.location_id

    {% if is_incremental() %}
    cross join latest_pickup_ts as lpts
    -- only process new trips based on pickup datetime
    where tp.pickup_datetime > lpts.max_ts
    {% endif %}
)
select * from processed_trips