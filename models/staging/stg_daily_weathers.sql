with source as (
    select
        *
    from
        {{ source("bigquery", "daily_weathers") }}
),
stg as (
    select
        cast(replace(weather_date, '/', '-') as date) as weather_date,
        day_of_week,
        average_temperature,
        precipitation,
        weather_noon,
        weather_night
    from
        source
)
select
    *
from
    stg