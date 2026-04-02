with source_data as (
    select *
    from {{ source('pbdb_raw', 'intervals') }}
),

renamed as (
    select
        oid as interval_id,
        tsc as timescale_id,
        nam as interval_name,
        itp as interval_type,
        pid as parent_interval_id,
        abr as abbreviation,
        col as color_hex,
        lag as latest_age_ma,
        eag as early_age_ma,
        rid as reference_id,
        current_timestamp() as _dbt_loaded_at
    from source_data
)

{{ dbt_utils.deduplicate(
    relation='renamed',
    partition_by='interval_id',
    order_by='timescale_id'
) }}
