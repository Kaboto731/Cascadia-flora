"""Data tests for the species seed. These run in CI because the seed is committed."""

import csv
from pathlib import Path

import pytest

SEED = Path(__file__).resolve().parents[1] / "dbt" / "seeds" / "species_list.csv"
EXPECTED_COLUMNS = ["species_key", "scientific_name", "common_name", "species_group"]


@pytest.fixture(scope="module")
def rows():
    assert SEED.exists(), f"Seed file not found: {SEED}. Run scripts/build_species_seed.py"
    with SEED.open(newline="") as f:
        return list(csv.DictReader(f))


def test_has_expected_columns(rows):
    assert list(rows[0].keys()) == EXPECTED_COLUMNS


def test_not_empty(rows):
    assert len(rows) > 0


def test_no_null_or_blank_values(rows):
    for row in rows:
        for col in EXPECTED_COLUMNS:
            assert row[col] is not None and row[col].strip() != "", f"blank {col} in {row}"


def test_keys_are_positive_integers(rows):
    for row in rows:
        assert row["species_key"].isdigit(), f"non-integer key: {row}"
        assert int(row["species_key"]) > 0


def test_no_duplicate_keys(rows):
    keys = [r["species_key"] for r in rows]
    assert len(keys) == len(set(keys)), "duplicate species_key in seed"


def test_no_duplicate_names(rows):
    names = [r["scientific_name"] for r in rows]
    assert len(names) == len(set(names)), "duplicate scientific_name in seed"
