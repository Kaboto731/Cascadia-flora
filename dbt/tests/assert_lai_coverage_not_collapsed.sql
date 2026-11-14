{{ config(severity='warn') }}
-- Cities, water and rock legitimately have no LAI, but most records of forest and woodland
-- trees should. If fewer than 40% of records have a value, the raster probably does not
-- line up with the occurrences (wrong tile, wrong projection). Returns one row when it happens.

select
    count(*) filter (where has_lai) as n_with_lai,
    count(*)                        as n_total
from {{ ref('int_occurrences__lai') }}
having count(*) filter (where has_lai) < 0.4 * count(*)
