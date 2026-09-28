{{
  config(
    materialized='incremental',
    unique_key=['requested_hour', 'pickup_zone_key'],
    incremental_strategy='delete+insert',
    pre_hook="""
      {% if is_incremental() %}
        delete from {{ this }}
        where requested_hour in (
            select distinct requested_hour
            from {{ ref('fact_ride') }}
            where _ingested_at > {{ high_watermark() }}
        );
      {% endif %}
    """
  )
}}

with affected_hours as (
    select distinct requested_hour
    from {{ ref('fact_ride') }}
    {% if is_incremental() %}
    where _ingested_at > {{ high_watermark() }}
    {% endif %}
),

relevant_facts as (
    select f.*
    from {{ ref('fact_ride') }} f
    inner join affected_hours h
      on f.requested_hour = h.requested_hour
)

select
    requested_hour,
    pickup_zone_key,
    count(*) as rides_requested,
    count(*) filter (where status = 'completed') as rides_completed,
    count(*) filter (where status = 'cancelled') as rides_cancelled,
    coalesce(sum(total_amount) filter (where status = 'completed'), 0)::numeric(12, 2) as gross_revenue,
    coalesce(sum(tip_amount) filter (where status = 'completed'), 0)::numeric(12, 2) as tips,
    avg(wait_seconds) as avg_wait_seconds,
    max(_ingested_at) as _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from relevant_facts
group by requested_hour, pickup_zone_key
