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
        c.tokyo_crimes_id,
        c.teguchi,
        c.city,
        c.victim_sex,
        c.victim_age,
        c.occurrence_time,
        c.occurrence_date,
        c.occurrence_month,
        case
            when c.occurrence_time_num is null then null
            when c.occurrence_time_num < 2 then '00-02時'
            when c.occurrence_time_num < 4 then '02-04時'
            when c.occurrence_time_num < 6 then '04-06時'
            when c.occurrence_time_num < 8 then '06-08時'
            when c.occurrence_time_num < 10 then '08-10時'
            when c.occurrence_time_num < 12 then '10-12時'
            when c.occurrence_time_num < 14 then '12-14時'
            when c.occurrence_time_num < 16 then '14-16時'
            when c.occurrence_time_num < 18 then '16-18時'
            when c.occurrence_time_num < 20 then '18-20時'
            when c.occurrence_time_num < 22 then '20-22時'
            when c.occurrence_time_num < 24 then '22-24時'
        end as occurrence_time_str,
        cm.longitude,
        cm.latitude,
        dw.day_of_week,
        dw.average_temperature_level,
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