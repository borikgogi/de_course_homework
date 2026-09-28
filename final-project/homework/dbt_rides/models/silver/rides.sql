-- silver.rides — ЕТАП 2. Grain: одна поїздка (ride_id). SPEC.md, розділ 4.3.
--   * incremental, unique_key='ride_id', delete+insert
--   * перебудовуйте поїздки, яких торкнулися НОВІ події, з УСІЄЇ їхньої історії в ref('events')
--   * поїздка існує, коли прийшла її ride_requested; порядок прибуття подій байдужий
--   * status, фактична зона (перекриває заявлену), wait_seconds, trip_seconds, суми з ride_completed
--   * _ingested_at = максимум _ingested_at усіх подій поїздки

{{
  config(
    materialized='incremental',
    unique_key='ride_id',
    incremental_strategy='delete+insert'
  )
}}

with relevant_rides as (
    select distinct ride_id
    from {{ ref('events') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}
),

events_filtered as (
    select e.*
    from {{ ref('events') }} e
    inner join relevant_rides r on e.ride_id = r.ride_id
),

events_pivot as (
    select
        ride_id,
        -- requested
        max(case when event_type = 'ride_requested' then payload->'rider'->>'id' end) as rider_id,
        max(case when event_type = 'ride_requested' then payload->'rider'->>'platform' end) as rider_platform,
        max(case when event_type = 'ride_requested' then payload->'rider'->>'app_version' end) as app_version,
        max(case when event_type = 'ride_requested' then payload->>'requested_vehicle' end) as requested_vehicle,
        max(case when event_type = 'ride_requested' then (payload->>'surge_estimate')::numeric end) as surge_estimate,
        max(case when event_type = 'ride_requested' then (payload->'pickup'->>'zone_id')::int end) as req_pickup_zone_id,
        max(case when event_type = 'ride_requested' then (payload->'dropoff'->>'zone_id')::int end) as req_dropoff_zone_id,
        max(case when event_type = 'ride_requested' then occurred_at end) as requested_at,

        -- accepted
        max(case when event_type = 'ride_accepted' then payload->'driver'->>'id' end) as acc_driver_id,
        max(case when event_type = 'ride_accepted' then occurred_at end) as accepted_at,

        -- started
        max(case when event_type = 'ride_started' then payload->'driver'->>'id' end) as start_driver_id,
        max(case when event_type = 'ride_started' then (payload->'pickup'->>'zone_id')::int end) as act_pickup_zone_id,
        max(case when event_type = 'ride_started' then occurred_at end) as started_at,

        -- completed
        max(case when event_type = 'ride_completed' then payload->'driver'->>'id' end) as comp_driver_id,
        max(case when event_type = 'ride_completed' then (payload->'dropoff'->>'zone_id')::int end) as act_dropoff_zone_id,
        max(case when event_type = 'ride_completed' then (payload->>'distance_km')::numeric(8,2) end) as distance_km,
        max(case when event_type = 'ride_completed' then (payload->'fare'->>'surge_multiplier')::numeric(4,2) end) as surge_multiplier,
        max(case when event_type = 'ride_completed' then (payload->'fare'->>'amount')::numeric(10,2) end) as fare_amount,
        max(case when event_type = 'ride_completed' then (payload->'fare'->>'tolls')::numeric(10,2) end) as tolls_amount,
        max(case when event_type = 'ride_completed' then (payload->'fare'->>'tip')::numeric(10,2) end) as tip_amount,
        max(case when event_type = 'ride_completed' then (payload->'fare'->>'total')::numeric(10,2) end) as total_amount,
        max(case when event_type = 'ride_completed' then payload->'fare'->>'currency' end) as currency,
        max(case when event_type = 'ride_completed' then occurred_at end) as completed_at,

        -- cancelled
        max(case when event_type = 'ride_cancelled' then payload->>'cancelled_by' end) as cancelled_by,
        max(case when event_type = 'ride_cancelled' then payload->>'reason' end) as cancel_reason,
        max(case when event_type = 'ride_cancelled' then payload->>'stage' end) as cancel_stage,
        max(case when event_type = 'ride_cancelled' then occurred_at end) as cancelled_at,

        -- payment
        max(case when event_type = 'payment_captured' then payload->'payment'->>'method' end) as payment_method,
        max(case when event_type = 'payment_captured' then (payload->'payment'->>'amount')::numeric(10,2) end) as payment_amount,
        max(case when event_type = 'payment_captured' then payload->'payment'->>'psp_reference' end) as psp_reference,
        max(case when event_type = 'payment_captured' then occurred_at end) as paid_at,

        -- technical metadata
        max(_ingested_at) as _ingested_at
    from events_filtered
    group by ride_id
)

select
    ride_id,
    rider_id,
    rider_platform,
    app_version,
    requested_vehicle,
    coalesce(comp_driver_id, start_driver_id, acc_driver_id) as driver_id,
    case
        when completed_at is not null then 'completed'
        when cancelled_at is not null then 'cancelled'
        when started_at is not null then 'in_progress'
        when accepted_at is not null then 'accepted'
        else 'requested'
    end as status,
    requested_at,
    accepted_at,
    started_at,
    completed_at,
    cancelled_at,
    paid_at,
    coalesce(act_pickup_zone_id, req_pickup_zone_id) as pickup_zone_id,
    coalesce(act_dropoff_zone_id, req_dropoff_zone_id) as dropoff_zone_id,
    case
        when accepted_at is not null and requested_at is not null
        then extract(epoch from (accepted_at - requested_at))::int
    end as wait_seconds,
    case
        when completed_at is not null and started_at is not null
        then extract(epoch from (completed_at - started_at))::int
    end as trip_seconds,
    surge_estimate,
    distance_km,
    surge_multiplier,
    fare_amount,
    tolls_amount,
    tip_amount,
    total_amount,
    currency,
    cancelled_by,
    cancel_reason,
    cancel_stage,
    payment_method,
    payment_amount,
    psp_reference,
    _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from events_pivot
where requested_at is not null
