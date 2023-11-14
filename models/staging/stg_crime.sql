with source as (
    select
        *
    from
        {{ source("bigquery", "crime") }}
),
stg as (
    select
        row_number()over() as key_id,
        zaimei,
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
)
select
    *
from
    stg