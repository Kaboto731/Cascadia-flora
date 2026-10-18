"""Guards the study area: the box used for the GBIF download and the polygon seed for dbt."""

import csv
from pathlib import Path

import pytest

from pipelines.study_area import STUDY_BOX, box_wkt

SEED = Path(__file__).resolve().parents[1] / "dbt" / "seeds" / "study_area.csv"


def test_box_wkt_is_a_closed_ring():
    coords = box_wkt().removeprefix("POLYGON((").removesuffix("))").split(", ")
    assert len(coords) == 5
    assert coords[0] == coords[-1]


def test_box_wkt_is_counter_clockwise():
    # GBIF rejects clockwise polygons. Shoelace formula: positive area means counter-clockwise.
    pts = [
        tuple(map(float, c.split()))
        for c in box_wkt().removeprefix("POLYGON((").removesuffix("))").split(", ")
    ]
    area = sum(x1 * y2 - x2 * y1 for (x1, y1), (x2, y2) in zip(pts, pts[1:], strict=False))
    assert area > 0


def test_seed_fits_dbt_field_limit():
    # dbt's seed loader fails on any single CSV field over 131,072 characters. The WKT is
    # almost the whole file, so the file size is a close, conservative proxy.
    assert SEED.stat().st_size < 131_072, "study_area.csv too large for dbt seed; simplify more"


@pytest.fixture(scope="module")
def polygon():
    shapely_wkt = pytest.importorskip("shapely.wkt")
    with SEED.open(newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1, "study_area seed must hold exactly one polygon"
    return shapely_wkt.loads(rows[0]["wkt"])


def test_polygon_is_valid_and_inside_the_download_box(polygon):
    west, south, east, north = STUDY_BOX
    assert polygon.is_valid
    pw, ps, pe, pn = polygon.bounds
    assert west <= pw and ps >= south and pe <= east and pn <= north


@pytest.mark.parametrize(
    ("name", "lon", "lat"),
    [
        ("Seattle", -122.33, 47.61),
        ("Spokane", -117.43, 47.66),
        ("Portland", -122.68, 45.52),
        ("Bend", -121.31, 44.06),
        ("Ashland", -122.71, 42.19),
        ("Victoria BC", -123.37, 48.43),
        ("Tofino BC", -125.90, 49.15),
        ("Vancouver BC", -123.12, 49.28),
    ],
)
def test_polygon_contains_study_area_cities(polygon, name, lon, lat):
    from shapely.geometry import Point

    assert polygon.contains(Point(lon, lat)), name


@pytest.mark.parametrize(
    ("name", "lon", "lat"),
    [
        ("Boise ID", -116.20, 43.62),
        ("Coeur d'Alene ID", -116.78, 47.68),
        ("Eureka CA", -124.16, 40.80),
        ("Calgary AB", -114.07, 51.05),
        ("open Pacific", -127.5, 46.0),
    ],
)
def test_polygon_excludes_neighbors_and_open_ocean(polygon, name, lon, lat):
    from shapely.geometry import Point

    assert not polygon.contains(Point(lon, lat)), name
