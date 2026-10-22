-- Why records were rejected, per species. A record can have several reasons, so the
-- counts across reasons overlap and do not sum to the number of rejected records
-- (see fct_dq_summary for that).

with exploded as (
    select species_name, unnest(rejection_reasons) as reason
    from {{ ref('int_occurrences__flagged') }}
),

totals as (
    select species_name, count(*) as n_species_records
    from {{ ref('int_occurrences__flagged') }}
    group by 1
)

select
    e.species_name,
    e.reason,
    count(*)                                              as n_records,
    t.n_species_records,
    round(100.0 * count(*) / t.n_species_records, 2)      as pct_of_species_records
from exploded as e
inner join totals as t using (species_name)
group by e.species_name, e.reason, t.n_species_records
order by e.species_name, n_records desc
