with source_data as (
    select *
    from {{ source('pbdb_raw', 'occurrence_opinions') }}
),

renamed as (
    select
        oid as opinion_id,
        otp as opinion_type_code,
        rnk as taxonomic_rank_code,
        nam as taxon_name,
        tid as taxon_id,
        vid as name_variant_id,
        sta as opinion_status,
        prl as parent_taxon_name,
        par as parent_taxon_id,
        spl as spelling_reason,
        oat as opinion_author,
        opy as opinion_publication_year,
        rid as reference_id,
        current_timestamp() as _dbt_loaded_at
    from source_data
)

select * from renamed
