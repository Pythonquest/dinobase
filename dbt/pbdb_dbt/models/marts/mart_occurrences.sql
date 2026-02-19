with enriched as (
    select * from {{ ref('int_occurrences_enriched') }}
)

select
    -- Primary identifiers
    occurrence_id,
    collection_id,
    reference_id,

    -- Taxonomic information
    identified_name,
    taxon_id,
    taxon_name,
    taxonomic_difference_flag,
    taxonomic_rank,
    phylum,
    taxon_class,
    taxon_order,
    family,
    genus,

    -- Temporal information
    early_age_ma,
    early_interval,
    late_interval,
    latest_age_ma,
    midpoint_age_ma,
    interval_id,

    -- First / last appearance
    first_appearance_early_ma,
    first_appearance_late_ma,
    last_appearance_early_ma,
    last_appearance_late_ma,

    -- Modern coordinates
    longitude,
    latitude,

    -- Paleocoordinates
    paleo_longitude,
    paleo_latitude,
    paleo_model,
    geoplate_id,

    -- Location
    collection_name,
    country_code,
    state_province,
    county,

    -- Stratigraphy
    formation,
    strat_group,
    strat_member,

    -- Environment
    environment,

    -- Data quality
    record_flags,

    -- Metadata
    _dbt_loaded_at

from enriched
