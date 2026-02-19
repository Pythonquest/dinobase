with occurrences as (
    select * from {{ ref('int_occurrences_enriched') }}
    where midpoint_age_ma is not null
),

time_bins as (
    select
        bin_start,
        bin_start + 10 as bin_end,
        bin_start + 5.0 as bin_midpoint
    from unnest(generate_array(0, 540, 10)) as bin_start
),

binned as (
    select
        t.bin_start,
        t.bin_end,
        t.bin_midpoint,
        o.phylum,
        o.taxon_class,
        o.taxon_order,
        o.family,
        o.genus,
        o.taxon_name,
        o.environment
    from occurrences o
    inner join time_bins t
        on o.midpoint_age_ma >= t.bin_start
        and o.midpoint_age_ma < t.bin_end
)

select
    bin_start,
    bin_end,
    bin_midpoint,

    count(*) as total_occurrences,
    count(distinct genus) as genus_richness,
    count(distinct family) as family_richness,
    count(distinct taxon_order) as order_richness,
    count(distinct taxon_class) as class_richness,
    count(distinct phylum) as phylum_richness,
    count(distinct taxon_name) as species_name_count,

    count(distinct case when environment like '%marine%' then genus end) as marine_genus_richness,
    count(distinct case when environment not like '%marine%' then genus end) as non_marine_genus_richness

from binned
group by bin_start, bin_end, bin_midpoint
order by bin_start
