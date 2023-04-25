{{ config(
    materialized='view',
    engine='MergeTree()',
    order_by='start_datetime',
    partition_by='toYYYYMMDD(start_datetime)',
) }}

with bike_point_db as (
    select
        Id as station_id,
        TerminalName as station_id_
    from {{ source('staging', 'bike_point') }}

    union all

    select
        Id as station_id,
        Id as station_id_
    from {{ source('staging', 'bike_point') }}
)

select
    rental_id,
    bike_id,
    sbp.station_id as start_station_id,
    start_station_name,
    start_datetime,
    ebp.station_id as end_station_id,
    end_station_name,
    end_datetime,
    end_datetime - start_datetime as duration
from (
    select *
    from {{ source('staging', 'usage_stats') }}
) as db_raw
inner join bike_point_db as sbp
on sbp.station_id_ = db_raw.start_station_id
inner join bike_point_db as ebp
on ebp.station_id_ = db_raw.end_station_id
where duration >= 0 and duration <= 86400

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 1000

{% endif %}