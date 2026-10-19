"""Offline tests for the GBIF download request. No network or credentials needed."""

from pipelines.ingest_gbif import STUDY_AREA_WKT, build_predicate


def _by_key(predicate, key):
    return [p for p in predicate["predicates"] if p.get("key") == key]


def test_predicate_includes_all_species_as_strings():
    pred = build_predicate([111, 222, 333])
    (taxon,) = _by_key(pred, "TAXON_KEY")
    assert taxon["type"] == "in"
    assert taxon["values"] == ["111", "222", "333"]


def test_predicate_has_quality_filters():
    pred = build_predicate([1])
    assert pred["type"] == "and"
    assert _by_key(pred, "HAS_COORDINATE")[0]["value"] == "true"
    assert _by_key(pred, "HAS_GEOSPATIAL_ISSUE")[0]["value"] == "false"
    assert _by_key(pred, "OCCURRENCE_STATUS")[0]["value"] == "PRESENT"


def test_predicate_has_geometry():
    pred = build_predicate([1])
    (geo,) = [p for p in pred["predicates"] if p["type"] == "within"]
    assert geo["geometry"] == STUDY_AREA_WKT


def _ring(wkt):
    body = wkt.removeprefix("POLYGON((").removesuffix("))").split(", ")
    return [tuple(float(v) for v in pair.split()) for pair in body]


def test_study_area_polygon_is_closed():
    ring = _ring(STUDY_AREA_WKT)
    assert ring[0] == ring[-1]
