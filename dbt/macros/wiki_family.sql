{# Maps a Wikimedia wiki code (e.g. enwiki, dewiktionary, commonswiki) to its project family. #}
{% macro wiki_family(col) %}
    case
        when {{ col }} = 'commonswiki'   then 'commons'
        when {{ col }} = 'wikidatawiki'  then 'wikidata'
        when {{ col }} = 'metawiki'      then 'meta'
        when {{ col }} = 'specieswiki'   then 'species'
        when {{ col }} = 'mediawikiwiki' then 'mediawiki'
        when {{ col }} like '%wiktionary' then 'wiktionary'
        when {{ col }} like '%wikisource' then 'wikisource'
        when {{ col }} like '%wikibooks'  then 'wikibooks'
        when {{ col }} like '%wikiquote'  then 'wikiquote'
        when {{ col }} like '%wikinews'   then 'wikinews'
        when {{ col }} like '%wikiversity' then 'wikiversity'
        when {{ col }} like '%wikivoyage' then 'wikivoyage'
        when {{ col }} like '%wikimedia'  then 'wikimedia'
        when {{ col }} like '%wiki'       then 'wikipedia'
        else 'other'
    end
{% endmacro %}
