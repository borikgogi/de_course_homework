-- Крок 2: silver.commits. Специфікація: ../../SPEC.md → «Крок 2».
-- Джерело: {{ ref('events') }}, лише PushEvent.
-- from_json(payload, PUSH_SCHEMA) → explode масиву commits → commit grain. PUSH_SCHEMA = var('push_schema').
-- Дедуп: один рядок на commit_sha, найраніший pushed_at.
-- Колонки: commit_sha, repo_name, pushed_by, branch, author_name, author_email, message,
--          is_distinct, pushed_at, is_merge_commit, message_subject, message_length
-- Пастка: `distinct` — reserved word, у DDL-схемі та доступі до поля потрібні backticks.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
with push_events as (
    select
        event_id,
        actor_login,
        repo_name,
        created_at,
        from_json(
            payload,
            'struct<size:int, distinct_size:int, ref:string, commits:array<struct<sha:string, message:string, `distinct`:boolean, author:struct<name:string, email:string>>>>'
        ) as p
    from {{ ref('events') }}
    where event_type = 'PushEvent'
),

exploded_commits as (
    select
        event_id,
        actor_login as pushed_by,
        repo_name,
        created_at as pushed_at,
        regexp_replace(p.ref, '^refs/heads/', '') as branch,
        explode(p.commits) as c
    from push_events
),

flattened as (
    select
        c.sha as commit_sha,
        repo_name,
        pushed_by,
        branch,
        c.author.name as author_name,
        c.author.email as author_email,
        c.message as message,
        c.`distinct` as is_distinct,
        pushed_at,
        case when c.message like 'Merge %' then true else false end as is_merge_commit,
        split(c.message, '\n')[0] as message_subject,
        length(c.message) as message_length,
        row_number() over (
            partition by c.sha
            order by pushed_at asc, event_id asc
        ) as rn
    from exploded_commits
    where c.sha is not null
)

select
    commit_sha,
    repo_name,
    pushed_by,
    branch,
    author_name,
    author_email,
    message,
    is_distinct,
    pushed_at,
    is_merge_commit,
    message_subject,
    message_length
from flattened
where rn = 1

