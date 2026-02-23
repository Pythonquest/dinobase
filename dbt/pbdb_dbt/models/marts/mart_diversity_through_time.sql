with occurrences as (
    select * from {{ ref('int_occurrences_enriched') }}
    where time_bin_start is not null
)

select
    time_bin_start as bin_start,
    time_bin_start + 10 as bin_end,
    time_bin_start + 5.0 as bin_midpoint,

    count(*) as total_occurrences,
    count(distinct genus) as genus_richness,
    count(distinct family) as family_richness,
    count(distinct taxon_order) as order_richness,
    count(distinct taxon_class) as class_richness,
    count(distinct phylum) as phylum_richness,
    count(distinct taxon_name) as species_name_count,

    count(distinct case when environment like '%marine%' then genus end) as marine_genus_richness,
    count(distinct case when environment not like '%marine%' then genus end) as non_marine_genus_richness

from occurrences
{{ dbt_utils.group_by(n=3) }}
order by bin_start
