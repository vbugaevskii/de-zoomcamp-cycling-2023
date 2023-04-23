{{ config(materialized='table') }}

select
    toStartOfDay(start_datetime) as dt,
    start_station_id,
    end_station_id,
    count(*) as rides_cnt,
    sum(duration) as rides_duration
from {{ ref('stg_rides_info') }}
group by dt, start_station_id, end_station_id
order by dt asc, rides_cnt desc