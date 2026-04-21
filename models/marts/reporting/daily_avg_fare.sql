WITH
is_weekend_added AS (
    SELECT
        DATE_TRUNC('month', pickup_datetime) AS pickup_month,
        DATE_TRUNC('day', pickup_datetime) AS pickup_date,
        DAYOFWEEK(pickup_datetime) IN (0, 6) AS pickup_day_is_weekend,
        fare_amount,
        total_amount
    FROM {{ ref('fct_trips') }}
)
SELECT
    -- Grouping dimensions
    pickup_month,

    -- Revenue breakdown (summed by zone, month, and service type)
    AVG(fare_amount) AS avg_daily_fare,
    AVG(CASE WHEN pickup_day_is_weekend THEN fare_amount ELSE 0 END) avg_daily_weekend_fare,
    AVG(CASE WHEN NOT pickup_day_is_weekend THEN fare_amount ELSE 0 END) avg_daily_workday_fare,
    AVG(total_amount) AS avg_daily_total_fee,
    AVG(CASE WHEN pickup_day_is_weekend THEN total_amount ELSE 0 END) avg_daily_weekend_total_fee,
    AVG(CASE WHEN NOT pickup_day_is_weekend THEN total_amount ELSE 0 END) avg_daily_workday_total_fee

FROM is_weekend_added

GROUP BY pickup_month