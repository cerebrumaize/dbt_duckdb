SELECT
    MIN(pickup_datetime) AS min_pu_ts,
    MAX(pickup_datetime) AS max_pu_ts
FROM {{ ref("int_trips_unioned") }};

