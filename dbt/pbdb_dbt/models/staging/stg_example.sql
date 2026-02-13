-- Example staging model
-- Replace this with your actual staging models that reference pbdb_raw sources

{{
    config(
        materialized='view',
        schema='pbdb_analytics'
    )
}}

-- Example: This would reference a source table from pbdb_raw
-- Uncomment and modify when you have actual source tables
/*
with source_data as (
    select *
    from {{ source('pbdb_raw', 'your_raw_table') }}
),

cleaned as (
    select
        -- Add your transformations here
        *,
        current_timestamp() as _dbt_loaded_at
    from source_data
)

select * from cleaned
*/

-- Placeholder query until sources are defined
select 
    1 as id,
    'example' as name,
    current_timestamp() as created_at
