{{ config(materialized='table') }}

select
    toStartOfDay(start_datetime) as dt,
    bike_id,
    sum(duration) as rides_duration,
    count(rental_id) as rides_cnt
from {{ ref('stg_rides_info') }}
group by bike_id, dt
order by dt asc, rides_duration desc