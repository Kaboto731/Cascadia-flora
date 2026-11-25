-- Spatial thinning: keep one record per species per grid cell.
--
-- Why: iNaturalist records pile up around cities and trails. Ten records in the same 1 km
-- pixel carry the same climate, so they overcount that place and teach a model about observer
-- habits instead of habitat. One record per cell removes that duplication. Cells are 1/N
-- degree on a fixed grid (var thin_cells_per_degree; 120 lines up exactly with WorldClim's 30s
-- pixels), so thinning and the climate raster agree on what "one place" means.
--
-- Of the records sharing a cell, keep the one with the smallest coordinate uncertainty, then
-- the most recent. Only records with climate are kept, since the models need it.
-- Use the unthinned int_occurrences__climate for density and trend analyses.

{% set cells = var('thin_cells_per_degree', 120) %}

with gridded as (

    select
        *,
        cast(floor(lon * {{ cells }}) as integer) as cell_x,
        cast(floor(lat * {{ cells }}) as integer) as cell_y
    from {{ ref('int_occurrences__climate') }}
    where has_climate

)

select *
from gridded
qualify row_number() over (
    partition by species_key, cell_x, cell_y
    order by uncertainty_m nulls last, event_date desc nulls last, gbif_id
) = 1
