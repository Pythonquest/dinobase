with enriched as (
    select * from {{ ref('int_occurrences_enriched') }}
)

select
    {{ dbt_utils.star(from=ref('int_occurrences_enriched')) }}
from enriched
