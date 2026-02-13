{{
    config(
        materialized='view',
        schema='pbdb_analytics'
    )
}}

with source_data as (
    select *
    from {{ source('pbdb_raw', 'occurrences') }}
),

renamed as (
    select
        -- Primary identifiers
        oid as occurrence_id,
        cid as collection_id,
        rid as reference_id,

        -- Taxonomic information
        tna as taxon_name,
        tid as taxon_id,
        rnk as taxonomic_rank,
        idn as identified_name,
        tdf as taxonomic_difference_flag,

        -- Temporal information
        oei as early_interval,
        iid as interval_id,
        eag as early_age_ma,
        lag as latest_age_ma,
        case
            when eag is not null and lag is not null
            then (eag + lag) / 2.0
            else coalesce(eag, lag)
        end as midpoint_age_ma,

        -- Geographic information
        oli as original_location_identifier,

        -- Data quality
        flg as record_flags,

        -- dbt metadata
        current_timestamp() as _dbt_loaded_at

    from source_data
)

select * from renamed
