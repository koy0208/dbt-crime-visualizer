with source as (
    select
        *
    from
        {{ source("bigquery", "tokyo_crimes") }}
),
stg as (
    select
        row_number()over() as tokyo_crimes_id,
        teguchi,
        prefecture,
        city,
        cyoume,
        {{ replace_null("victim_sex") }} as victim_sex,
        {{ replace_null("victim_age") }} as victim_age,
        {{ replace_null("victim_job") }} as victim_job,
        cast(replace(occurrence_date, '/', '-') as date) as occurrence_date,
        occurrence_time
    from
        source
    where
        occurrence_date not in ('不明')
        and prefecture != ''
)
select
    *
from
    stg