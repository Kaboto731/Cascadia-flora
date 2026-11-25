-- How much clustering did thinning remove? One row per species.
-- pct_kept near 100 means records were already spread out; a low number means most records
-- were extra observations in places already represented (typically cities and trails).

select
    c.species_name,
    count(*)                                                       as n_clean_deduped,
    count(*) filter (where c.has_climate)                          as n_with_climate,
    any_value(t.n_thinned)                                         as n_thinned,
    round(100.0 * any_value(t.n_thinned) / count(*) filter (where c.has_climate), 1)
                                                                   as pct_kept
from {{ ref('int_occurrences__climate') }} as c
left join (
    select species_name, count(*) as n_thinned
    from {{ ref('int_occurrences__thinned') }}
    group by species_name
) as t using (species_name)
group by c.species_name
order by n_clean_deduped desc
