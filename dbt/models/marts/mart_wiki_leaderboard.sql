-- Which wikis are most active, and how bot-driven are they?
select
    wiki,
    wiki_family,
    count(*)                                              as events,
    count(*) filter (where event_type in ('edit', 'new')) as edits,
    count(distinct user_name)                             as distinct_users,
    round(100.0 * count(*) filter (where is_bot) / count(*), 1) as pct_bot,
    rank() over (order by count(*) desc)                  as activity_rank
from {{ ref('stg_wiki_events') }}
group by 1, 2
order by activity_rank
