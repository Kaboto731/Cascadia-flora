-- Per species, thinned records can never outnumber the cleaned, deduplicated ones, and every
-- thinned record must have climate. Returns offending species.

with thinned as (
    select species_key, count(*) as n_thinned, count(*) filter (where not has_climate) as n_no_climate
    from {{ ref('int_occurrences__thinned') }}
    group by species_key
),
deduped as (
    select species_key, count(*) as n_deduped
    from {{ ref('int_occurrences__deduped') }}
    group by species_key
)

select t.species_key, t.n_thinned, d.n_deduped, t.n_no_climate
from thinned as t
left join deduped as d using (species_key)
where t.n_thinned > coalesce(d.n_deduped, 0)
   or t.n_no_climate > 0
