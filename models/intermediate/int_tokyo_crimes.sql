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
        {{ kanji_to_num("occurrence_address") }} as occurrence_address_num,
        case
            when occurrence_time_num is null then null
            when occurrence_time_num < 2 then '00-02時'
            when occurrence_time_num < 4 then '02-04時'
            when occurrence_time_num < 6 then '04-06時'
            when occurrence_time_num < 8 then '06-08時'
            when occurrence_time_num < 10 then '08-10時'
            when occurrence_time_num < 12 then '10-12時'
            when occurrence_time_num < 14 then '12-14時'
            when occurrence_time_num < 16 then '14-16時'
            when occurrence_time_num < 18 then '16-18時'
            when occurrence_time_num < 20 then '18-20時'
            when occurrence_time_num < 22 then '20-22時'
            when occurrence_time_num < 24 then '22-24時'
        end as occurrence_time_str,
    from
        add_columns
)
select
    *
from
    fin