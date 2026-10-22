-- Per-species funnel: all records -> clean -> clean after deduplication.

select
    f.species_name,
    count(*)                                                    as n_total,
    count(*) filter (where f.is_clean)                          as n_clean,
    count(*) filter (where not f.is_clean)                      as n_rejected,
    count(*) filter (where f.is_clean and len(f.review_flags) > 0)  as n_clean_with_review_flags,
    -- coalesce: a species with no clean records has no row in the deduped table (0, not NULL)
    coalesce(any_value(d.n_deduped), 0)                         as n_clean_deduped,
    round(100.0 * count(*) filter (where f.is_clean) / count(*), 1) as pct_clean
from {{ ref('int_occurrences__flagged') }} as f
left join (
    select species_name, count(*) as n_deduped
    from {{ ref('int_occurrences__deduped') }}
    group by 1
) as d using (species_name)
group by f.species_name
order by n_total desc
