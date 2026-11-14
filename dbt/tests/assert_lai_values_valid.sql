-- MODIS LAI is stored as 0-100 and scaled by 0.1, so real values lie in 0-10. Anything else
-- means the scale factor or the fill-code masking broke. The best composite can never be
-- lower than the mean, and a value needs at least one composite behind it.
-- Returns offending rows; NULL LAI (no data) is not an error and is not checked here.

select gbif_id, lai_summer_mean, lai_summer_max, lai_n_obs
from {{ ref('int_occurrences__lai') }}
where lai_summer_mean is not null
  and (
      lai_summer_mean not between 0 and 10
      or lai_summer_max not between 0 and 10
      or lai_summer_max < lai_summer_mean - 0.0001
      or lai_n_obs < 1
  )
