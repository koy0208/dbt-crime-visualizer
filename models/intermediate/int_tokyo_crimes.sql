with stg as (
    select
        *
    from
        {{ ref("stg_tokyo_crimes") }}
),
add_columns as (
    select
        *,
        prefecture || city || cyoume as occurrence_address,
        date_trunc(occurrence_date, year) as occurrence_year,
        date_trunc(occurrence_date, month) as occurrence_month,
        cast(
            case
                when occurrence_time = '不明' then null
                else occurrence_time
            end as int
        ) as occurrence_time_num
    from
        stg
    where
        occurrence_date >= '2018-04-01'
),
fin as (
    select
        *,
        {{ kanji_to_num("occurrence_address") }} as occurrence_address_num
    from
        add_columns
)
select
    *
from
    fin