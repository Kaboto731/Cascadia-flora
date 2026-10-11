"""Offline tests for the bronze load. They use a tiny fake GBIF file, no network needed."""

import csv

import duckdb
import pytest

from pipelines.bronze_gbif import (
    MANIFEST_COLUMNS,
    append_manifest,
    baseline_stats,
    csv_to_parquet,
    find_raw_file,
)

HEADER = [
    "gbifID",
    "species",
    "decimalLatitude",
    "decimalLongitude",
    "coordinateUncertaintyInMeters",
    "basisOfRecord",
    "year",
    "countryCode",
    "stateProvince",
]
ROWS = [
    r.split("|")
    for r in [
        "1|Quercus garryana|47.6|-122.3|30|HUMAN_OBSERVATION|2020|US|Washington",
        # A stray double quote in a text field must not break parsing or drop the row.
        '2|Quercus garryana|48.4|-123.4||PRESERVED_SPECIMEN|1985|CA|B.C. "Vic"',
        "3|Pinus albicaulis|47.0|-121.5|100|HUMAN_OBSERVATION|2015|US|Washington",
    ]
]


@pytest.fixture
def raw_file(tmp_path):
    path = tmp_path / "0001-test.csv"
    path.write_text("\n".join("\t".join(r) for r in [HEADER, *ROWS]) + "\n")
    return path


def test_csv_to_parquet_keeps_every_row(raw_file, tmp_path):
    dest = tmp_path / "out" / "occ.parquet"
    assert csv_to_parquet(raw_file, dest) == len(ROWS)


def test_bronze_columns_are_all_text(raw_file, tmp_path):
    dest = tmp_path / "occ.parquet"
    csv_to_parquet(raw_file, dest)
    types = duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{dest}')").fetchall()
    assert {t[1] for t in types} == {"VARCHAR"}


def test_find_raw_file_picks_the_data_file(raw_file, tmp_path):
    (tmp_path / "citations.txt").write_text("x")
    assert find_raw_file(tmp_path) == raw_file


def test_manifest_is_idempotent(tmp_path):
    path = tmp_path / "manifest.csv"
    row = dict.fromkeys(MANIFEST_COLUMNS, "x")
    row["download_key"] = "0001"
    assert append_manifest(row, path) is True
    assert append_manifest(row, path) is False
    with path.open(newline="") as f:
        assert len(list(csv.DictReader(f))) == 1


def test_baseline_stats_mentions_species_and_totals(raw_file, tmp_path):
    dest = tmp_path / "occ.parquet"
    csv_to_parquet(raw_file, dest)
    text = baseline_stats(dest)
    assert "Total records:** 3" in text
    assert "Quercus garryana" in text
    assert "Pinus albicaulis" in text
