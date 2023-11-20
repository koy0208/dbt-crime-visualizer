with source as (
    select
        *
    from
        {{ source("bigquery", "coordinate_masters") }}
),
stg as (
    select
        prefecture,
        city,
        cyoume,
        longitude,
        latitude,
    from
        source
)
select
    *
from
    stg