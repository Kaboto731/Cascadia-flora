-- No record that survives cleaning may exceed the uncertainty limit.
-- Returns offending rows; the test passes when there are none.

select gbif_id, uncertainty_m
from {{ ref('int_occurrences__deduped') }}
where uncertainty_m > {{ var('max_uncertainty_m') }}
