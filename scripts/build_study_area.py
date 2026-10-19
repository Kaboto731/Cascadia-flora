"""Build the exact study-area polygon used by dbt.

Takes the Washington, Oregon and British Columbia boundaries from Natural Earth (public
domain), unions them, adds shoreline slack (without crossing into neighboring states),
clips to STUDY_BOX, simplifies, and writes one WKT row to dbt/seeds/study_area.csv.

Requires shapely (pip install shapely). The generated seed is committed, so you only
need to rerun this if you change the study area.

    python scripts/build_study_area.py
"""

import csv
import json
import sys
from pathlib import Path

import requests
from shapely import to_wkt
from shapely.geometry import box, shape
from shapely.ops import unary_union

from pipelines.study_area import STUDY_BOX, STUDY_REGIONS

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/"
    "geojson/ne_50m_admin_1_states_provinces.geojson"
)
CACHE = REPO_ROOT / "data" / "raw" / "natural_earth" / "ne_50m_admin_1.geojson"
OUTPUT = REPO_ROOT / "dbt" / "seeds" / "study_area.csv"

# Generalized coastlines can sit several km from the real shore (the 50 m data puts
# Tofino, BC about 11 km offshore), so the study area gets generous slack. To keep that
# slack from leaking inland across the real borders, the neighbors' land is subtracted:
# borders stay strict, only water gets the extra room. 0.15 degrees is about 16 km.
SLACK_DEG = 0.15
SIMPLIFY_DEG = 0.01
NEIGHBOR_REGIONS = ("US-ID", "US-CA", "US-NV", "US-MT", "CA-AB")


def load_boundaries() -> dict:
    if not CACHE.exists():
        CACHE.parent.mkdir(parents=True, exist_ok=True)
        resp = requests.get(SOURCE_URL, timeout=120)
        resp.raise_for_status()
        CACHE.write_bytes(resp.content)
    return json.loads(CACHE.read_text())


def build_polygon():
    features = load_boundaries()["features"]
    wanted = {f["properties"]["iso_3166_2"]: f for f in features}
    missing = [c for c in (*STUDY_REGIONS, *NEIGHBOR_REGIONS) if c not in wanted]
    if missing:
        raise SystemExit(f"Regions not found in Natural Earth data: {missing}")

    region = unary_union([shape(wanted[c]["geometry"]) for c in STUDY_REGIONS])
    neighbors = unary_union([shape(wanted[c]["geometry"]) for c in NEIGHBOR_REGIONS])
    area = region.buffer(SLACK_DEG).difference(neighbors).intersection(box(*STUDY_BOX))
    return area.simplify(SIMPLIFY_DEG, preserve_topology=True)


def main() -> int:
    geom = build_polygon()
    wkt = to_wkt(geom, rounding_precision=4)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "wkt"])
        writer.writerow(["cascadia", wkt])

    vertices = sum(len(p.exterior.coords) for p in getattr(geom, "geoms", [geom]))
    print(f"Wrote {OUTPUT}")
    print(f"  type={geom.geom_type}  parts={len(getattr(geom, 'geoms', [geom]))}")
    print(f"  vertices={vertices:,}  wkt_size={len(wkt) / 1024:.0f} KB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
