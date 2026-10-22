{{ config(severity='warn') }}

-- Warns when a species ends up with too few clean records to model (whitebark pine may).
-- The threshold is a rule of thumb, not a hard requirement.

select
    s.species_key,
    s.scientific_name,
    coalesce(c.n_clean, 0) as n_clean
from {{ ref('species_list') }} as s
left join (
    select species_key, count(*) as n_clean
    from {{ ref('int_occurrences__deduped') }}
    group by 1
) as c using (species_key)
where coalesce(c.n_clean, 0) < 100
