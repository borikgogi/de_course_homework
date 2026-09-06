-- =====================================================================
-- TASK 4 — daily_activity_change (12 балів). Специфікація: ../../MODELS.md → «daily_activity_change».
-- Зміна кількості подій день-до-дня: LAG(...) OVER (ORDER BY ...).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
WITH ccount_events AS (
	SELECT 
		event_date,
		COUNT(*) as events
	FROM {{ ref('stg_events') }}
	GROUP BY event_date 
	ORDER BY event_date 
)
SELECT 
	event_date,
	events ,
	LAG(events) OVER (ORDER BY event_date) as prev_day_events,
	events - LAG(events) OVER (ORDER BY event_date) as delta_events
FROM ccount_events
