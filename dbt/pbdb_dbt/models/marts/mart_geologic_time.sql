-- Chronostratigraphic dimension: intervals within their time scale (ICS and regional scales)
with intervals as (
    select * from {{ ref('stg_intervals') }}
),

timescales as (
    select * from {{ ref('stg_timescales') }}
),

joined as (
    select
        i.interval_id,
        i.interval_name,
        i.interval_type,
        i.parent_interval_id,
        i.abbreviation,
        i.color_hex,
        i.early_age_ma,
        i.latest_age_ma,
        i.reference_id as interval_reference_id,
        i.timescale_id,
        ts.timescale_name,
        ts.geographic_scope as timescale_scope,
        ts.early_age_ma as timescale_early_age_ma,
        ts.latest_age_ma as timescale_latest_age_ma,
        ts.reference_id as timescale_reference_id,
        i._dbt_loaded_at
    from intervals i
    left join timescales ts on i.timescale_id = ts.timescale_id
)

{{ dbt_utils.deduplicate(
    relation='joined',
    partition_by='interval_id',
    order_by='timescale_id'
) }}
