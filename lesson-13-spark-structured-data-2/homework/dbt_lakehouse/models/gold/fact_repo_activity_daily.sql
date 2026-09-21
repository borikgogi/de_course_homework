-- Крок 10: gold.fact_repo_activity_daily. Специфікація: ../../SPEC.md → «Крок 10».
-- Грануляція: (repo_id, date_id). Багатоджерельний rollup з {{ ref('commits') }},
-- {{ ref('pull_requests') }}, {{ ref('issues') }} та {{ ref('events') }} (WatchEvent/ForkEvent).
-- Патерн: денний агрегат на джерело (метрика + нулі для решти) → union all → group by.
-- Відсутні метрики → 0, не NULL. Порядок і типи колонок у всіх CTE мають збігатися.
-- Колонки: activity_id (md5(concat_ws('|', repo_id, date_id))), repo_id, date_id, commits,
--          distinct_committers, prs_opened, prs_merged, issues_opened, issues_closed, stars, forks.

-- TODO: замініть заглушку на запит згідно зі SPEC.md
-- depends_on: {{ ref('commits') }}
-- depends_on: {{ ref('pull_requests') }}
-- depends_on: {{ ref('issues') }}
-- depends_on: {{ ref('events') }}
with commit_activity as (
    select
    md5(repo_name) as repo_id,
    cast(date_format(pushed_at, 'yyyyMMdd') as int) as date_id,
    count(*) as commits,
    count(distinct author_email) as distinct_committers,
        0 as prs_opened,
        0 as prs_merged,
        0 as issues_opened,
        0 as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('commits') }}
    group by 1, 2
),

pr_opened_activity as (
    select
        md5(repo_name) as repo_id,
        cast(date_format(opened_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        count(*) as prs_opened,
        0 as prs_merged,
        0 as issues_opened,
        0 as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('pull_requests') }}
    group by 1, 2
),

pr_merged_activity as (
    select
        md5(repo_name) as repo_id,
        cast(date_format(merged_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        0 as prs_opened,
        count(*) as prs_merged,
        0 as issues_opened,
        0 as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('pull_requests') }}
    where merged_at is not null
    group by 1, 2
),

issue_opened_activity as (
    select
        md5(repo_name) as repo_id,
        cast(date_format(opened_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        0 as prs_opened,
        0 as prs_merged,
        count(*) as issues_opened,
        0 as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('issues') }}
    where opened_at >= '2018-03-01'
    group by 1, 2
),

issue_closed_activity as (
    select
        md5(repo_name) as repo_id,
        cast(date_format(closed_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        0 as prs_opened,
        0 as prs_merged,
        0 as issues_opened,
        count(*) as issues_closed,
        0 as stars,
        0 as forks
    from {{ ref('issues') }}
    where closed_at is not null
    group by 1, 2
),

watch_fork_activity as (
    select
        md5(repo_name) as repo_id,
        cast(date_format(created_at, 'yyyyMMdd') as int) as date_id,
        0 as commits,
        0 as distinct_committers,
        0 as prs_opened,
        0 as prs_merged,
        0 as issues_opened,
        0 as issues_closed,
        sum(case when event_type = 'WatchEvent' then 1 else 0 end) as stars,
        sum(case when event_type = 'ForkEvent' then 1 else 0 end) as forks
    from {{ ref('events') }}
    where event_type in ('WatchEvent', 'ForkEvent')
    group by 1, 2
),

combined as (
    select * from commit_activity
    union all
    select * from pr_opened_activity
    union all
    select * from pr_merged_activity
    union all
    select * from issue_opened_activity
    union all
    select * from issue_closed_activity
    union all
    select * from watch_fork_activity
)

select
    md5(concat_ws('|', repo_id, cast(date_id as string))) as activity_id,
    repo_id,
    date_id,
    sum(commits) as commits,
    sum(distinct_committers) as distinct_committers,
    sum(prs_opened) as prs_opened,
    sum(prs_merged) as prs_merged,
    sum(issues_opened) as issues_opened,
    sum(issues_closed) as issues_closed,
    sum(stars) as stars,
    sum(forks) as forks
from combined
group by
    repo_id,
    date_id