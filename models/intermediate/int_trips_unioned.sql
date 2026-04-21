WITH green_tripdata AS (
    SELECT *
    FROM {{ ref('stg_green_tripdata') }}
    WHERE pickup_datetime <= strptime('2026-02-28', '%Y-%m-%d')
        AND pickup_datetime >= strptime('2025-01-01', '%Y-%m-%d')
),

yellow_tripdata AS (
    SELECT *
    FROM {{ ref('stg_yellow_tripdata') }}
    WHERE pickup_datetime <= strptime('2026-02-28', '%Y-%m-%d')
        AND pickup_datetime >= strptime('2025-01-01', '%Y-%m-%d')
)

SELECT * FROM green_tripdata
UNION ALL
SELECT * FROM yellow_tripdata
