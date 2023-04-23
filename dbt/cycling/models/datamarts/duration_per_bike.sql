{{ config(materialized='table') }}

select
    toStartOfDay(start_datetime) as dt,
    bike_id,
    sum(duration) as usage_sec,
    count(rental_id) as usage_cnt
from {{ ref('stg_rides_info') }}
group by bike_id, dt
order by dt asc, usage_sec desc