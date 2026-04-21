SELECT
    DATE_TRUNC('day', pickup_datetime) AS pickup_date,
    DAYOFWEEK(pickup_datetime) AS pickup_dayofweek,
    fare_amount,
    total_amount
FROM {{ ref('fct_trips') }}
;
