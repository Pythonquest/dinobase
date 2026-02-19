with source_data as (
    select *
    from {{ source('pbdb_raw', 'collections') }}
),

renamed as (
    select
        -- Primary identifiers
        oid as collection_id,
        rid as reference_id,
        nam as collection_name,

        -- Temporal information
        oei as early_interval,
        oli as late_interval,
        eag as early_age_ma,
        lag as latest_age_ma,

        -- Modern coordinates
        lng as longitude,
        lat as latitude,

        -- Paleocoordinates
        pln as paleo_longitude,
        pla as paleo_latitude,
        pm1 as paleo_model,
        gpl as geoplate_id,

        -- Location
        cc2 as country_code,
        stp as state_province,
        cny as county,

        -- Stratigraphy
        sfm as formation,
        sgr as strat_group,
        smb as strat_member,

        -- Environment
        env as environment,

        -- dbt metadata
        current_timestamp() as _dbt_loaded_at

    from source_data
)

select * from renamed
