{{ config(materialized='table') }}

select
    toStartOfDay(start_datetime) as dt,
    count(*)
from {{ ref('stg_rides_info') }}
group by dt
order by dt asc