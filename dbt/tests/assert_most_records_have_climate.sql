{{ config(severity='warn') }}
-- A few records legitimately have no climate (offshore, outside the raster). More than 2%
-- means the raster does not line up with the occurrences. Returns one row when it happens.

select
    count(*) filter (where not has_climate) as n_missing,
    count(*)                                as n_total
from {{ ref('int_occurrences__climate') }}
having count(*) filter (where not has_climate) > 0.02 * count(*)
