-- Thinning must leave at most one record per species per grid cell. Returns offending cells.

select species_key, cell_x, cell_y, count(*) as n_records
from {{ ref('int_occurrences__thinned') }}
group by species_key, cell_x, cell_y
having count(*) > 1
