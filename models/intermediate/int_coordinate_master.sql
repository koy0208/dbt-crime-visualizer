with stg as (
    select
        *
    from
        {{ ref("stg_coordinate_master") }}
),
int1 as (
    select
        *,
        prefecture || city || cyoume as address
    from
        stg
),
int2 as (
    select
        *,
        {{ kanji_to_num("address") }} as address_num
    from
        int1
),
fin as (
    select
        address_num,
        avg(longitude) as longitude,
        avg(latitude) as latitude
    from
        int2
    group by
        address_num
)
select
    *
from
    fin