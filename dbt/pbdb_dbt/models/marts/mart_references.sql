-- Bibliographic dimension: join key is reference_id (ref:...) for occurrences, collections, taxa, opinions
select * from {{ ref('stg_references') }}
