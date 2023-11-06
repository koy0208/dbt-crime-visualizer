with
    source as (select * from {{ source("bigquery", "crime") }}),
    stg as (
        select
            zaimei,
            teguchi,
            prefecture,
            city,
            cyoume,
            prefecture || city || cyoume as address,
            victim_sex,
            victim_age,
            victim_job,
            cast(replace(occurrence_date, '/', '-') as date) as occurrence_date,
            occurrence_time
        from source
        where occurrence_date not in ('不明')
    ),
    fin as (
        select
            *,
            date_trunc(occurrence_date, year) as occurrence_year,
            date_trunc(occurrence_date, month) as occurrence_month,
            cast(
                case
                    when occurrence_time = '不明' then null else occurrence_time
                end as numeric
            ) as occurrence_time_nu
        from stg

    )
select *
from fin
