with stg as (
    select
        *
    from
        {{ ref("stg_daily_weathers") }}
),
fin as (
    select
        *,
        case 
            when precipitation >= 50 then '50mm以上'
            when precipitation >= 10 then '10-49mm'
            when precipitation >= 1 then '1-9mm'
            when precipitation = 0 then '0mm'
        end as precipitation_level
    from
        stg
)
select
    *
from
    fin