-- Most active human (non-bot) editors.
select
    user_name,
    count(*)                       as edits,
    count(distinct wiki)           as wikis_touched,
    coalesce(sum(length_delta), 0) as net_bytes,
    min(event_ts)                  as first_seen,
    max(event_ts)                  as last_seen
from {{ ref('stg_wiki_events') }}
where not is_bot
  and event_type in ('edit', 'new')
group by 1
order by edits desc
limit 100
