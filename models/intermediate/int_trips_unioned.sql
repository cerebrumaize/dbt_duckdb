with green_tripdata as (
    select * from {{ ref('stg_green_tripddata') }}
),

yellow_tripdata as (
    select * from {{ ref('stg_yellow_tripdata') }}
)

select * from green_tripdata
union all
select * from yellow_tripdata
