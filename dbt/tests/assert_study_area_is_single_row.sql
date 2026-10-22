-- The study-area check cross joins one polygon row. If the seed were empty, every record
-- would silently disappear; if it had two rows, every record would be duplicated.

select count(*) as n_rows
from {{ ref('study_area') }}
having count(*) != 1
