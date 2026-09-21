-- Крок 3: silver.pull_requests. Специфікація: ../../SPEC.md → «Крок 3».
-- Джерело: {{ ref('events') }}, лише PullRequestEvent. from_json(payload, PR_SCHEMA), PR_SCHEMA = var('pr_schema').
-- Грануляція: один рядок на (repo_name, pr_number) — стан з ОСТАННЬОЇ за часом події (row_number desc).
-- Колонки: repo_name, pr_number, title, author_login, state, is_merged, is_draft, opened_at,
--          closed_at, merged_at, additions, deletions, changed_files, commits_count, comments,
--          review_comments, author_association, label_names, last_action, last_event_at, churn, hours_open

-- TODO: замініть заглушку на запит згідно зі SPEC.md
with pr_events as (
    select
        event_id,
        repo_name,
        created_at as event_at,
        from_json(
            payload,
            'struct<action:string, number:int, pull_request:struct<state:string, title:string, draft:boolean, merged:boolean, created_at:string, closed_at:string, merged_at:string, additions:int, deletions:int, changed_files:int, commits:int, comments:int, review_comments:int, author_association:string, user:struct<login:string>, labels:array<struct<name:string>>>>'
        ) as p
    from {{ ref('events') }}
    where event_type = 'PullRequestEvent'
),

parsed as (
    select
        repo_name,
        p.number as pr_number,
        p.pull_request.title as title,
        p.pull_request.user.login as author_login,
        p.pull_request.state as state,
        coalesce(p.pull_request.merged, false) as is_merged,
        coalesce(p.pull_request.draft, false) as is_draft,
        to_timestamp(p.pull_request.created_at) as opened_at,
        to_timestamp(p.pull_request.closed_at) as closed_at,
        to_timestamp(p.pull_request.merged_at) as merged_at,
        p.pull_request.additions as additions,
        p.pull_request.deletions as deletions,
        p.pull_request.changed_files as changed_files,
        p.pull_request.commits as commits_count,
        p.pull_request.comments as comments,
        p.pull_request.review_comments as review_comments,
        p.pull_request.author_association as author_association,
        transform(p.pull_request.labels, x -> x.name) as label_names,
        p.action as last_action,
        event_at as last_event_at,
        (p.pull_request.additions + p.pull_request.deletions) as churn,
        cast(
            (
                unix_timestamp(coalesce(to_timestamp(p.pull_request.closed_at), event_at))
                - unix_timestamp(to_timestamp(p.pull_request.created_at))
            ) as double
        ) / 3600.0 as hours_open,
        row_number() over (
            partition by repo_name, p.number
            order by event_at desc, event_id desc
        ) as rn
    from pr_events
)

select
    repo_name,
    pr_number,
    title,
    author_login,
    state,
    is_merged,
    is_draft,
    opened_at,
    closed_at,
    merged_at,
    additions,
    deletions,
    changed_files,
    commits_count,
    comments,
    review_comments,
    author_association,
    label_names,
    last_action,
    last_event_at,
    churn,
    hours_open
from parsed
where rn = 1
