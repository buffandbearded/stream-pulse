-- Events timestamped more than 10 minutes in the future indicate a clock/data problem.
{{ config(severity='warn') }}
select event_id, event_ts
from {{ ref('stg_wiki_events') }}
where event_ts > now()::timestamp + interval 10 minute
