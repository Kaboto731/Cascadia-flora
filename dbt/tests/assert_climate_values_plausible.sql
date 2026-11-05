-- WorldClim values far outside anything possible in Cascadia mean a unit or alignment bug
-- (for example temperatures stored x10, or a shifted grid). Returns offending rows.
-- NULL climate is not checked here; assert_most_records_have_climate covers that.

select gbif_id, bio_1, bio_12
from {{ ref('int_occurrences__climate') }}
where bio_1 not between -30 and 40
   or bio_12 not between 0 and 12000
