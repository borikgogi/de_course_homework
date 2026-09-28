-- depends_on: {{ ref('dim_driver') }}
-- У dim_driver має бути рівно один технічний запис 'unknown'
with check_unknown as (
    select count(*) as cnt
    from {{ ref('dim_driver') }}
    where driver_key = 'unknown'
)
select cnt
from check_unknown
where cnt != 1
