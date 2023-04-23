{{ config(materialized='table') }}

select
    toStartOfDay(start_datetime) as dt,
    count(*) as rides_cnt,
    sum(duration) as rides_duration
from {{ ref('stg_rides_info') }}
group by dt
order by dt asc