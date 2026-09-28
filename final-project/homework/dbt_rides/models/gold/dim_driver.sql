{{
  config(
    materialized='incremental',
    unique_key='driver_key',
    incremental_strategy='delete+insert'
  )
}}

with accepted_events as (
    select
        payload->'driver'->>'id' as driver_key,
        payload->'driver'->'vehicle'->>'type' as vehicle_type,
        payload->'driver'->'vehicle'->>'medallion' as medallion,
        (payload->'driver'->>'rating')::numeric(3,2) as rating,
        occurred_at,
        _ingested_at
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
      and payload->'driver'->>'id' is not null
),

drivers_to_update as (
    select distinct driver_key
    from accepted_events
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}
),

history as (
    select e.*
    from accepted_events e
    inner join drivers_to_update u on e.driver_key = u.driver_key
),

ranked as (
    select
        driver_key,
        vehicle_type,
        medallion,
        rating as latest_rating,
        min(occurred_at) over (partition by driver_key) as first_seen_at,
        max(occurred_at) over (partition by driver_key) as last_seen_at,
        max(_ingested_at) over (partition by driver_key) as _ingested_at,
        row_number() over (
            partition by driver_key
            order by occurred_at desc, _ingested_at desc
        ) as rn
    from history
)

select
    driver_key,
    vehicle_type,
    medallion,
    latest_rating,
    first_seen_at,
    last_seen_at,
    _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from ranked
where rn = 1

union all

select
    'unknown' as driver_key,
    'Unknown' as vehicle_type,
    'Unknown' as medallion,
    null as latest_rating,
    '1970-01-01 00:00:00+00'::timestamptz as first_seen_at,
    '1970-01-01 00:00:00+00'::timestamptz as last_seen_at,
    '1970-01-01 00:00:00+00'::timestamptz as _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
{% if is_incremental() %}
where not exists (
    select 1 from {{ this }} where driver_key = 'unknown'
)
{% endif %}
