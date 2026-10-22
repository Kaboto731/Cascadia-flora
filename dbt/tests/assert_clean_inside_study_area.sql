-- Every clean record must be inside the study area.

select gbif_id, lat, lon
from {{ ref('int_occurrences__deduped') }}
where in_study_area is not true
