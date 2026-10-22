-- The cleaning rules. Flag, don't delete: every record keeps all of its reasons.
--   rejection_reasons : record is excluded from modeling (empty list = clean)
--   review_flags      : record is kept, but worth a look or a sensitivity check
-- Thresholds come from dbt_project.yml vars.

with base as (
    select * from {{ ref('int_occurrences__geo') }}
),

flagged as (
    select
        *,
        list_filter([
            case when lat is null or lon is null
                then 'missing_coordinates' end,
            case when lat = lon or (lat = 0 and lon = 0)
                then 'zero_or_swapped_coords' end,
            case when in_study_area is false
                then 'outside_study_area' end,
            case when uncertainty_m > {{ var('max_uncertainty_m') }}
                then 'uncertainty_too_high' end,
            case when least(lat_decimals, lon_decimals) < {{ var('min_coordinate_decimals') }}
                then 'coordinates_too_coarse' end,
            case when basis_of_record in ('FOSSIL_SPECIMEN', 'LIVING_SPECIMEN', 'MATERIAL_SAMPLE')
                then 'bad_basis_of_record' end,
            case when establishment_means = 'MANAGED'
                then 'cultivated' end,
            case when event_year is null
                    or event_year < {{ var('min_year') }}
                    or event_year > year(current_date)
                then 'year_out_of_range' end,
            case when contains(coalesce(issue, ''), 'COUNTRY_COORDINATE_MISMATCH')
                    or contains(coalesce(issue, ''), 'PRESUMED_SWAPPED_COORDINATE')
                    or contains(coalesce(issue, ''), 'PRESUMED_NEGATED_LONGITUDE')
                then 'gbif_geospatial_issue' end,
            case when contains(coalesce(issue, ''), 'TAXON_MATCH_NONE')
                    or contains(coalesce(issue, ''), 'TAXON_MATCH_HIGHERRANK')
                then 'low_taxon_confidence' end
        ], x -> x is not null) as rejection_reasons,

        list_filter([
            -- Roughly 20% of records have no uncertainty. They pass the uncertainty rule
            -- (unknown is not the same as too high), but are flagged so a sensitivity
            -- run without them is easy.
            case when uncertainty_m is null then 'uncertainty_missing' end
        ], x -> x is not null) as review_flags
    from base
)

select
    *,
    len(rejection_reasons) = 0                    as is_clean,
    array_to_string(rejection_reasons, ';')       as rejection_reason_text,
    array_to_string(review_flags, ';')            as review_flag_text
from flagged
