with source_data as (
    select *
    from {{ source('pbdb_raw', 'timescales') }}
),

renamed as (
    select
        oid as timescale_id,
        nam as timescale_name,
        lag as latest_age_ma,
        eag as early_age_ma,
        loc as geographic_scope,
        rid as reference_id,
        current_timestamp() as _dbt_loaded_at
    from source_data
)

select * from renamed
