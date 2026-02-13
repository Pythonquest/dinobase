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
        cid as collection_id,
        oid as occurrence_id,
        rid as reference_id,

        -- Taxonomic information
        idn as identified_name,
        tid as taxon_id,
        tna as taxon_name,
        tdf as taxonomic_difference_flag,
        rnk as taxonomic_rank,

        -- Temporal information
        eag as early_age_ma,
        oei as early_interval,
        iid as interval_id,
        lag as latest_age_ma,
        oli as original_location_identifier,

        -- Geographic information
        flg as record_flags,

        -- Data quality
        case
            when eag is not null and lag is not null
                then (eag + lag) / 2.0
            else coalesce(eag, lag)
        end as midpoint_age_ma,

        -- dbt metadata
        current_timestamp() as _dbt_loaded_at

    from source_data
)

select * from renamed
