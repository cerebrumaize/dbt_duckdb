WITH
day_added AS (
    SELECT
        vendor_id,
        taxi_type,
        DATE_TRUNC('day', pickup_datetime) AS pickup_date,
        DAYOFWEEK(pickup_datetime) IN (0, 6) AS pickup_day_is_weekend,
        fare_amount,
        total_amount
    FROM {{ ref('fct_trips') }}
    WHERE fare_amount >= 0 AND total_amount > 0
)
SELECT
    -- Grouping dimensions
    dv.vendor_name,
    taxi_type,
    pickup_date,
    ANY_VALUE(pickup_day_is_weekend) AS pickup_day_is_weekend,
    -- Revenue breakdown (summed by zone, month, and service type)
    AVG(fare_amount) AS avg_daily_fare,
    AVG(total_amount) AS avg_daily_total,
    AVG(100.0*fare_amount/total_amount) AS avg_daily_fare_ratio

FROM day_added AS da
JOIN {{ ref("dim_vendors") }} AS dv ON da.vendor_id = dv.vendor_id

GROUP BY dv.vendor_name,
    taxi_type,
    pickup_date