-- one row per trip
-- add a PK GUID trip_id
-- find all the duplicates and fix them
-- enrich payment_type

/* check if there are duplicates
-- below query returns 50, which means there are more than 50 rows that are duplicated.
select *, count(*)
from {{ ref("int_trips_unioned") }}
group by all
having count(*) > 1
limit 50
*/
-- select count(1) from {{ ref("int_trips") }} -- 112,086,662
-- select count(1) from (select distinct * from {{ ref("int_trips_unioned") }} ) -- 114,827,122
-- select 'all_count', count(1) from {{ref("int_trips_unioned")}} -- 114,827,251

