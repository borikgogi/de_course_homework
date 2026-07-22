-- =====================================================================
-- TASK 6 — mart_category_daily (20 балів). Специфікація: ../../MODELS.md → «mart_category_daily».
-- Широка вітрина: multi-join stg_events + event_categories + calendar, агрегація по (день × категорія).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
SELECT 
	se.event_date,
	c.is_weekend as is_weekend,
	ec.category as category,
	COUNT(*) as events,
	COUNT(DISTINCT se.repo_name) as distinct_repos,
	COUNT(DISTINCT se.actor_login) as distinct_actors
FROM {{ ref('stg_events') }} AS se 
JOIN {{ ref('calendar') }} AS c 
	ON se.event_date = c.day
JOIN {{ ref('event_categories') }} AS ec 
	ON se.event_type = ec.event_type 
GROUP BY 
	se.event_date,
	c.is_weekend,
	ec.category
ORDER BY 	
	se.event_date
