-- Classification opinions with accepted taxon hierarchy for subject and parent (PBDB current taxonomy)
with opinions as (
    select * from {{ ref('stg_taxonomic_opinions') }}
),

taxa as (
    select * from {{ ref('stg_taxa') }}
)

select
    o.opinion_id,
    o.opinion_type_code,
    o.taxonomic_rank_code as opinion_subject_rank_code,
    o.taxon_name as opinion_subject_name,
    o.taxon_id,
    o.name_variant_id,
    o.opinion_status,
    o.parent_taxon_name as opinion_parent_name,
    o.parent_taxon_id,
    o.spelling_reason,
    o.opinion_author,
    o.opinion_publication_year,
    o.reference_id,

    ts.taxon_rank_code as accepted_subject_rank_code,
    ts.occurrence_count as subject_taxon_occurrence_count,
    ts.phylum as subject_phylum,
    ts.taxon_class as subject_class,
    ts.taxon_order as subject_order,
    ts.family as subject_family,
    ts.genus as subject_genus,

    tp.taxon_name as parent_accepted_name,
    tp.taxon_rank_code as parent_accepted_rank_code,

    o._dbt_loaded_at

from opinions o
left join taxa ts on o.taxon_id = ts.taxon_id
left join taxa tp on o.parent_taxon_id = tp.taxon_id
