with occurrences as (
    select * from {{ ref('stg_occurrences') }}
),

enriched as (
    select
        -- Primary identifiers
        collection_id,
        occurrence_id,
        reference_id,

        -- Taxonomic information
        identified_name,
        taxon_id,
        taxon_name,
        taxonomic_difference_flag,
        taxonomic_rank,

        -- Temporal information
        early_age_ma,
        early_interval,
        interval_id,
        latest_age_ma,
        original_location_identifier,

        -- Calculated temporal fields
        case
            when early_age_ma is not null and latest_age_ma is not null
                then (early_age_ma + latest_age_ma) / 2.0
            else coalesce(early_age_ma, latest_age_ma)
        end as midpoint_age_ma,

        -- Data quality
        record_flags,

        -- dbt metadata
        _dbt_loaded_at

    from occurrences
)

select * from enriched
