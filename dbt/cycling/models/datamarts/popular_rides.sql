{{ config(materialized='table') }}

select
    toStartOfDay(start_datetime) as dt,
    start_station_id,
    end_station_id,
    count(*) as cnt_rides
from {{ ref('stg_rides_info') }}
group by dt, start_station_id, end_station_id
order by dt asc, cnt_rides desc