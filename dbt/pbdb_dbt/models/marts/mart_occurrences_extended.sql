-- Occurrence facts plus resolved reference metadata and chronostratigraphic interval (when interval_id is set)
with occ as (
    select * from {{ ref('int_occurrences_enriched') }}
),

refs as (
    select
        reference_id,
        publication_type,
        reference_title,
        publication_year,
        author1_first_initial,
        author1_last_name,
        publication_name,
        volume,
        page_first,
        page_last,
        language
    from {{ ref('mart_references') }}
),

geo as (
    select
        interval_id,
        interval_name,
        interval_type,
        abbreviation as interval_abbreviation,
        early_age_ma as interval_early_age_ma,
        latest_age_ma as interval_latest_age_ma,
        timescale_name,
        timescale_scope
    from {{ ref('mart_geologic_time') }}
)

select
    occ.*,

    refs.publication_type,
    refs.reference_title,
    refs.publication_year,
    refs.author1_first_initial,
    refs.author1_last_name,
    refs.publication_name,
    refs.volume as reference_volume,
    refs.page_first as reference_page_first,
    refs.page_last as reference_page_last,
    refs.language as reference_language,

    geo.interval_name,
    geo.interval_type,
    geo.interval_abbreviation,
    geo.interval_early_age_ma,
    geo.interval_latest_age_ma,
    geo.timescale_name,
    geo.timescale_scope

from occ
left join refs on occ.reference_id = refs.reference_id
left join geo on occ.interval_id = geo.interval_id
