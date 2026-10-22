-- Rename and cast only. No filtering and no business logic.
-- Numeric casts use try_cast: a malformed value becomes NULL and is then caught by a
-- cleaning rule (and shows up in the audit table) instead of crashing the whole run.
-- gbifID is cast strictly because a bad ID should stop the run.

select
    cast(gbifID as bigint)                              as gbif_id,
    datasetKey                                          as dataset_key,
    try_cast(speciesKey as integer)                     as species_key,
    species                                             as species_name,
    countryCode                                         as country_code,
    stateProvince                                       as state_province,
    try_cast(decimalLatitude as double)                 as lat,
    try_cast(decimalLongitude as double)                as lon,
    -- Decimal places are read from the raw text: once cast to a number they are lost.
    length(split_part(decimalLatitude, '.', 2))         as lat_decimals,
    length(split_part(decimalLongitude, '.', 2))        as lon_decimals,
    try_cast(coordinateUncertaintyInMeters as double)   as uncertainty_m,
    try_cast(elevation as double)                       as elevation_m,
    basisOfRecord                                       as basis_of_record,
    establishmentMeans                                  as establishment_means,
    try_cast(year as integer)                           as event_year,
    try_cast(left(eventDate, 10) as date)               as event_date,
    recordedBy                                          as recorded_by,
    issue
from {{ source('bronze', 'gbif_occurrences') }}
