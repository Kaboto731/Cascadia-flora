-- Cleaned, deduplicated occurrences with summer leaf area index (LAI) at each point.
-- Left join: a record with no LAI is kept with NULLs and has_lai = false. That is expected
-- for records in cities, on water, bare rock or ice (MODIS publishes no LAI there), so an
-- empty value is information ("not a vegetated stand at 500 m"), not an error.
-- lai_summer_mean: mean of good July-August composites; lai_summer_max: the best composite;
-- lai_n_obs: how many composites fed the mean. LAI is in m2 of leaf per m2 of ground.

select
    d.*,
    l.lai_summer_mean,
    l.lai_summer_max,
    coalesce(l.lai_n_obs, 0)         as lai_n_obs,
    l.modis_tile,
    l.lai_summer_mean is not null    as has_lai
from {{ ref('int_occurrences__deduped') }} as d
left join {{ source('enrichment', 'modis_lai_points') }} as l using (gbif_id)
