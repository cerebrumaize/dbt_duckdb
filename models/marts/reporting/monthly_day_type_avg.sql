WITH
month_added AS (
    SELECT
        vendor_name,
        taxi_type,
        DATE_TRUNC('month', pickup_date)::DATE AS pickup_month,
        pickup_date,
        pickup_day_is_weekend,
        avg_daily_fare,
        avg_daily_total
    FROM {{ ref('int_daily_avg_fare') }}
)
SELECT
    -- Grouping dimensions
    vendor_name,
    taxi_type,
    pickup_month,

    -- Revenue breakdown (summed by zone, month, and service type)
    AVG(CASE WHEN pickup_day_is_weekend THEN avg_daily_fare ELSE 0 END) avg_monthly_weekend_fare,
    AVG(CASE WHEN NOT pickup_day_is_weekend THEN avg_daily_fare ELSE 0 END) avg_monthly_workday_fare,
    AVG(CASE WHEN pickup_day_is_weekend THEN avg_daily_total ELSE 0 END) avg_monthly_weekend_total,
    AVG(CASE WHEN NOT pickup_day_is_weekend THEN avg_daily_total ELSE 0 END) avg_monthly_workday_total,
    AVG(CASE WHEN pickup_day_is_weekend THEN avg_daily_total ELSE 0 END) avg_monthly_weekend_fare_ratio,
    AVG(CASE WHEN NOT pickup_day_is_weekend THEN avg_daily_total ELSE 0 END) avg_monthly_workday_fare_ratio

FROM month_added

GROUP BY vendor_name, taxi_type, pickup_month