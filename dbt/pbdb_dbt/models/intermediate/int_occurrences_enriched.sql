with occurrences as (
    select * from {{ ref('stg_occurrences') }}
),

collections as (
    select * from {{ ref('stg_collections') }}
),

taxa as (
    select * from {{ ref('stg_taxa') }}
),

enriched as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['o.occurrence_id']) }} as occurrence_surrogate_key,

        -- Occurrence identifiers
        o.occurrence_id,
        o.collection_id,
        o.reference_id,

        -- Taxonomic fields from occurrence record
        o.identified_name,
        o.taxon_id,
        o.taxon_name,
        o.taxonomic_difference_flag,
        o.taxonomic_rank,

        -- Classification hierarchy from taxa
        t.phylum,
        t.taxon_class,
        t.taxon_order,
        t.family,
        t.genus,

        -- Temporal (from occurrence)
        o.early_age_ma,
        o.early_interval,
        o.late_interval,
        o.latest_age_ma,
        o.interval_id,
        case
            when o.early_age_ma is not null and o.latest_age_ma is not null
                then (o.early_age_ma + o.latest_age_ma) / 2.0
            else coalesce(o.early_age_ma, o.latest_age_ma)
        end as midpoint_age_ma,

        cast(floor(
            case
                when o.early_age_ma is not null and o.latest_age_ma is not null
                    then (o.early_age_ma + o.latest_age_ma) / 2.0
                else coalesce(o.early_age_ma, o.latest_age_ma)
            end / 10
        ) * 10 as int64) as time_bin_start,

        -- First / last appearance from taxa
        t.first_appearance_early_ma,
        t.first_appearance_late_ma,
        t.last_appearance_early_ma,
        t.last_appearance_late_ma,

        -- Modern coordinates (occurrence-level, fallback to collection)
        coalesce(o.longitude, c.longitude) as longitude,
        coalesce(o.latitude, c.latitude) as latitude,

        -- Paleocoordinates (occurrence-level, fallback to collection)
        coalesce(o.paleo_longitude, c.paleo_longitude) as paleo_longitude,
        coalesce(o.paleo_latitude, c.paleo_latitude) as paleo_latitude,
        coalesce(o.paleo_model, c.paleo_model) as paleo_model,
        coalesce(cast(o.geoplate_id as string), c.geoplate_id) as geoplate_id,

        -- Location context from collection
        c.collection_name,
        c.country_code,
        c.state_province,
        c.county,

        -- Stratigraphy from collection
        c.formation,
        c.strat_group,
        c.strat_member,

        -- Environment from collection
        c.environment,

        -- Data quality
        o.record_flags,

        -- Metadata
        o._dbt_loaded_at

    from occurrences o
    left join collections c on o.collection_id = c.collection_id
    left join taxa t on o.taxon_id = t.taxon_id
)

select * from enriched
