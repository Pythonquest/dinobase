with source_data as (
    select *
    from {{ source('pbdb_raw', 'taxa') }}
),

renamed as (
    select
        -- Primary identifiers
        oid as taxon_id,
        nam as taxon_name,
        rnk as taxon_rank_code,
        noc as occurrence_count,
        par as parent_taxon_id,
        rid as reference_id,

        -- First / last appearance ages
        fea as first_appearance_early_ma,
        fla as first_appearance_late_ma,
        lea as last_appearance_early_ma,
        lla as last_appearance_late_ma,

        -- Classification hierarchy (resolved names from classext block)
        phl as phylum,
        cll as taxon_class,
        odl as taxon_order,
        fml as family,
        gnl as genus,

        -- Data quality
        flg as record_flags,

        -- dbt metadata
        current_timestamp() as _dbt_loaded_at

    from source_data
)

{{ dbt_utils.deduplicate(
    relation='renamed',
    partition_by='taxon_id',
    order_by='occurrence_count desc'
) }}
