with crime as (
    select
        *
    from
        {{ ref("int_crime") }}
),
coordinate_master as (
    select
        *
    from
        {{ ref("int_coordinate_master") }}
),
daily_weather as (
    select
        *
    from
        {{ ref("stg_daily_weather") }}
),
_join as (
    select
        c.*,
        cm.longitude,
        cm.latitude,
        dw.day_of_week,
        dw.average_temperature,
        dw.precipitation,
        dw.weather_noon
    from
        crime as c
        left join coordinate_master as cm on c.address_num = cm.address_num
        left join daily_weather as dw on c.occurrence_date = dw.weather_date
)
select
    *
from
    _join