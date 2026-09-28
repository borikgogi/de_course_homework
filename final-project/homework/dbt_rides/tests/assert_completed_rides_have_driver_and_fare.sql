-- depends_on: {{ ref('rides') }}
-- Завершені поїздки обов'язково повинні мати водія, невід'ємну відстань та вартість
select
    ride_id,
    driver_id,
    distance_km,
    fare_amount
from {{ ref('rides') }}
where status = 'completed'
  and (
      driver_id is null
      or distance_km is null
      or distance_km < 0
      or fare_amount is null
      or fare_amount < 0
  )
