-- =====================================================================
-- TASK 5 — starred_repos_without_push (12 балів). Специфікація: ../../MODELS.md → «starred_repos_without_push».
-- Репозиторії зі зіркою (WatchEvent), але без жодного PushEvent: anti-join (NOT EXISTS).
-- Контракт колонок нижче; заглушка повертає 0 рядків.
-- =====================================================================
WITH w_e AS( 
	SELECT DISTINCT
		repo_name,
		event_type 
	FROM {{ ref('stg_events') }}
	WHERE event_type = 'WatchEvent'

),
p_e AS (
	SELECT 
		repo_name,
		event_type
	FROM {{ ref('stg_events') }}
	WHERE event_type = 'PushEvent'
)

SELECT 
	w.repo_name,
--	w.event_type,
--	p.event_type
FROM w_e AS w
LEFT JOIN p_e AS p
	ON w.repo_name = p.repo_name
WHERE p.event_type IS NULL