"""The Cascadia study area: one definition shared by the GBIF download and dbt.

Study area = Washington + Oregon + British Columbia, clipped to STUDY_BOX.

- The GBIF download uses the box itself (cheap, loose, filtered upstream).
- scripts/build_study_area.py intersects the real WA/OR/BC boundaries with this box and
  writes dbt/seeds/study_area.csv, which dbt uses for the exact point-in-polygon check.

To change the study area, edit STUDY_BOX, rerun the build script, and request a new
GBIF download.
"""

# (west, south, east, north) in degrees. North of 49 is southern BC; 51 N covers the
# northern limit of Garry oak on Vancouver Island with margin. West of -124.9 is needed
# for the outer coast of Vancouver Island.
STUDY_BOX = (-128.5, 41.9, -116.4, 51.0)

# Regions included, as Natural Earth iso_3166_2 codes.
STUDY_REGIONS = ("US-WA", "US-OR", "CA-BC")


def box_wkt(box: tuple[float, float, float, float] = STUDY_BOX) -> str:
    """The box as a WKT polygon, counter-clockwise as GBIF requires."""
    west, south, east, north = box
    return (
        f"POLYGON(({west} {south}, {east} {south}, {east} {north}, {west} {north}, {west} {south}))"
    )
