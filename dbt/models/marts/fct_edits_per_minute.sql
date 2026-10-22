-- Tumbling one-minute windows over the event stream.
select
    event_minute,
    count(*)                                              as events,
    count(*) filter (where event_type in ('edit', 'new')) as edits,
    count(*) filter (where is_bot)                        as bot_events,
    count(*) filter (where not is_bot)                    as human_events,
    round(100.0 * count(*) filter (where is_bot) / count(*), 1) as pct_bot,
    count(*) filter (where event_type = 'new')            as new_pages,
    coalesce(sum(length_delta), 0)                        as net_bytes
from {{ ref('stg_wiki_events') }}
group by 1
order by 1
