-- Cleaned, deduplicated occurrences with the WorldClim climate at each point.
-- Left join: a record with no climate (open water, outside the raster) is kept with NULLs
-- and has_climate = false, so it shows up in a test instead of silently disappearing.
-- climate_snapped = true means a coastal point took the nearest land pixel (see
-- pipelines/ingest_worldclim.py). Units: temperatures in C, precipitation in mm.

select
    d.*,
    {% for n in range(1, 20) %}
    c.bio_{{ n }},
    {% endfor %}
    coalesce(c.climate_snapped, false) as climate_snapped,
    c.worldclim_res,
    c.bio_1 is not null as has_climate
from {{ ref('int_occurrences__deduped') }} as d
left join {{ source('enrichment', 'worldclim_points') }} as c using (gbif_id)
