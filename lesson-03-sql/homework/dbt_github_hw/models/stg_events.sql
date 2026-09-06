{{ config(materialized='view') }}

-- =====================================================================
-- TASK 1 — stg_events (12 балів). Специфікація: ../../MODELS.md → «stg_events».
-- Прочитати партиційований Parquet і застосувати DQ-фільтри (типи, боти, порожні push).
-- Нижче — лише контракт колонок (заглушка повертає 0 рядків). Замініть тіло запиту.
-- =====================================================================

WITH stg_events as (
	SELECT * FROM read_parquet('{{ var("events_path") }}', hive_partitioning = true)
)
SELECT 	
		id,
		event_type,
		created_at,
		event_date,
		actor_login,
		repo_name,
		payload_commit_count,
		payload_action,
		payload_ref
FROM stg_events 
WHERE  
	event_type IN ('PushEvent', 'IssuesEvent', 'PullRequestEvent', 'WatchEvent', 'IssueCommentEvent') 
	AND
	actor_login NOT LIKE '%[bot]' 
	AND NOT
	(event_type = 'PushEvent' AND	payload_commit_count = 0)
