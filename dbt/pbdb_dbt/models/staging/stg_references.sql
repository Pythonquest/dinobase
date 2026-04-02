with source_data as (
    select *
    from {{ source('pbdb_raw', 'refs') }}
),

renamed as (
    select
        oid as reference_id,
        pty as publication_type,
        tit as reference_title,
        pby as publication_year,
        ai1 as author1_first_initial,
        al1 as author1_last_name,
        pbt as publication_name,
        vol as volume,
        pgf as page_first,
        pgl as page_last,
        lan as language,
        current_timestamp() as _dbt_loaded_at
    from source_data
)

select * from renamed
