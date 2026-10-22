-- Deduplicated, typed, enriched events. One row per event_id.
with ranked as (
    select
        *,
        row_number() over (partition by event_id order by ingested_at) as rn
    from {{ source('raw', 'raw_events') }}
)

select
    event_id,
    event_type,
    wiki,
    {{ wiki_family('wiki') }}                         as wiki_family,
    title,
    user_name,
    is_bot,
    is_minor,
    namespace,
    -- net bytes changed by the event; null for events without a length (e.g. log entries)
    case
        when length_new is null then null
        else length_new - coalesce(length_old, 0)
    end                                               as length_delta,
    event_ts,
    date_trunc('minute', event_ts)                    as event_minute,
    ingested_at
from ranked
where rn = 1
