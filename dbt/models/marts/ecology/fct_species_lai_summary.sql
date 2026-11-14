-- How leafy is the stand around each species' records in summer?
-- Percentiles of the 500 m summer-mean LAI across clean, deduplicated records, plus how many
-- records sit in open (< 3) or dense (>= 5) cover. Only records with an LAI value count
-- toward the percentiles and shares; n_with_lai and pct_with_lai show how many that is.
-- These are unthinned records, so cities with many observers weigh more. LAI describes the
-- stand around a record (a 500 m pixel), not the tree itself, and saturates in very dense
-- forest, so differences between high values are weak evidence.

select
    species_name,
    count(*)                                                      as n_records,
    count(*) filter (where has_lai)                               as n_with_lai,
    round(100.0 * count(*) filter (where has_lai) / count(*), 1)  as pct_with_lai,
    round(quantile_cont(cast(lai_summer_mean as double), 0.10), 1) as lai_p10,
    round(median(cast(lai_summer_mean as double)), 1)             as lai_median,
    round(quantile_cont(cast(lai_summer_mean as double), 0.90), 1) as lai_p90,
    round(100.0 * count(*) filter (where lai_summer_mean < 3)
          / nullif(count(*) filter (where has_lai), 0), 1)        as pct_open_cover,
    round(100.0 * count(*) filter (where lai_summer_mean >= 5)
          / nullif(count(*) filter (where has_lai), 0), 1)        as pct_dense_cover
from {{ ref('int_occurrences__lai') }}
group by species_name
order by n_records desc
