WITH unioned_trips AS (
    SELECT * FROM {{ ref('int_trips_unioned') }}
),

vendors AS (
    SELECT
        DISTINCT vendor_id,
        {{ get_vendor_names('vendor_id') }} AS vendor_name
    FROM unioned_trips
)

SELECT * FROM vendors