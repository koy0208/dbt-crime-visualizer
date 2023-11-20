{{
  config(
    materialized='table',
    partition_by={
      "field": "occurrence_month",
      "data_type": "date",
      "granularity": "month"
    }
  )
}}
with crime as (
    select
        *
    from
        {{ ref("int_tokyo_crimes") }}
),
coordinate_master as (
    select
        *
    from
        {{ ref("dim_coordinate_masters") }}
),
daily_weather as (
    select
        *
    from
        {{ ref("int_daily_weathers") }}
),
_join as (
    select
        c.*,
        cm.longitude,
        cm.latitude,
        dw.day_of_week,
        dw.precipitation_level
    from
        crime as c
        left join coordinate_master as cm on c.occurrence_address_num = cm.address_num
        left join daily_weather as dw on c.occurrence_date = dw.weather_date
)
select
    *
from
    _join