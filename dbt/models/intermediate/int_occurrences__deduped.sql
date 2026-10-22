-- Clean records only, with duplicates removed.
-- The same observation often arrives through several datasets, so records with the same
-- species, coordinates (to ~10 m), date and observer count once. The most precise copy
-- wins; ties break on gbif_id so the result is deterministic.

select *
from {{ ref('int_occurrences__flagged') }}
where is_clean
qualify row_number() over (
    partition by species_key, round(lat, 4), round(lon, 4), event_date, recorded_by
    order by uncertainty_m nulls last, gbif_id
) = 1
