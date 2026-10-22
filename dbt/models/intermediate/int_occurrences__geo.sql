-- Adds one fact: is the point inside the Cascadia study area (WA + OR + BC)?
-- This is the only model that needs the DuckDB spatial extension, so it is kept separate
-- from the rules in int_occurrences__flagged.
-- Note ST_Point takes (x, y) = (longitude, latitude).

with area as (
    select ST_GeomFromText(wkt) as geom
    from {{ ref('study_area') }}
    where name = 'cascadia'
)

select
    o.*,
    case
        when o.lat is null or o.lon is null then null
        else ST_Within(ST_Point(o.lon, o.lat), area.geom)
    end as in_study_area
from {{ ref('stg_gbif__occurrences') }} as o
cross join area
