with stg as (
    select
        *
    from
        {{ ref("stg_crime") }}
),
int as (
    select
        *,
        prefecture || city || cyoume as address,
        date_trunc(occurrence_date, year) as occurrence_year,
        date_trunc(occurrence_date, month) as occurrence_month,
        cast(
            case
                when occurrence_time = '不明' then null
                else occurrence_time
            end as numeric
        ) as occurrence_time_num
    from
        stg
    where
        occurrence_date >= '2018-04-01'
),
fin as (
    select
        *,
        {{ kanji_to_num("address") }} as address_num
    from
        int
)
select
    *
from
    fin