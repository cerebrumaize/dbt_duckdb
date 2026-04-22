SELECT
    DISTINCT vendor_id
FROM {{ ref("fct_trips") }}
WHERE payment_type IS NOT NULL