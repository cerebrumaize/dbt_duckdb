select
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    extra,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge
from {{ ref('stg_yellow_tripdata') }}
where fare_amount < 0