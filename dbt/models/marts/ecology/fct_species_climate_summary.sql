-- Where each species lives in climate space: the 5th, 50th and 95th percentile of three
-- headline variables across its clean, deduplicated records.
--   bio_1  annual mean temperature (C)
--   bio_12 annual precipitation (mm)
--   bio_17 precipitation of the driest quarter (mm), a summer-drought proxy
-- Values are widened to double before rounding: the raster stores 32-bit floats.
-- Percentiles, not min/max: one bad coordinate should not stretch a species' niche.
-- These are unthinned records, so cities with many observers weigh more. This is a first
-- look, not the model input (that comes after spatial thinning).

select
    species_name,
    count(*)                                  as n_records,
    count(*) filter (where has_climate)       as n_with_climate,
    {% for col in ['bio_1', 'bio_12', 'bio_17'] %}
    round(quantile_cont(cast({{ col }} as double), 0.05), 1)  as {{ col }}_p05,
    round(median(cast({{ col }} as double)), 1)               as {{ col }}_median,
    round(quantile_cont(cast({{ col }} as double), 0.95), 1)  as {{ col }}_p95,
    {% endfor %}
    any_value(worldclim_res)                  as worldclim_res
from {{ ref('int_occurrences__climate') }}
group by species_name
order by n_records desc
