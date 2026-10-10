"""Resolve species names to GBIF taxon keys and write the dbt seed file.

Run from the repo root:
    python scripts/build_species_seed.py

Writes dbt/seeds/species_list.csv. Commit the result, so the pipeline is pinned to
known taxon keys instead of re-resolving names on every run.
"""

import csv
import sys
from pathlib import Path

import requests

MATCH_URL = "https://api.gbif.org/v1/species/match"
# Anchored to the repo root (this file lives in scripts/), so it works from any directory.
OUTPUT = Path(__file__).resolve().parents[1] / "dbt" / "seeds" / "species_list.csv"

# Starter set: abundant, rare, and high-elevation cases.
# Add more rows here later (western hemlock, bigleaf maple, ...) and re-run.
SPECIES = [
    {
        "scientific_name": "Pseudotsuga menziesii",
        "common_name": "Douglas-fir",
        "species_group": "lowland conifer",
    },
    {
        "scientific_name": "Quercus garryana",
        "common_name": "Garry oak",
        "species_group": "threatened",
    },
    {
        "scientific_name": "Pinus albicaulis",
        "common_name": "Whitebark pine",
        "species_group": "high elevation",
    },
]


def resolve(name: str) -> dict:
    """Look a name up in the GBIF backbone and return the raw match."""
    resp = requests.get(MATCH_URL, params={"name": name, "rank": "species"}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def main() -> int:
    rows, problems = [], []

    for entry in SPECIES:
        name = entry["scientific_name"]
        match = resolve(name)
        match_type = match.get("matchType")
        status = match.get("status")
        key = match.get("speciesKey") or match.get("usageKey")
        print(f"{name:25} key={key} matchType={match_type} status={status}")

        # Only accept exact matches to accepted names. Anything else needs a human.
        if key is None or match_type != "EXACT" or status != "ACCEPTED":
            problems.append(f"{name}: matchType={match_type}, status={status}, key={key}")
            continue

        rows.append({"species_key": int(key), **entry})

    if problems:
        print("\nNeeds review (not written to the seed):", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        print(
            "\nIf a name resolved to a synonym, look at match['acceptedUsageKey'] and use "
            "that key. If the keys above look missing, print the full match dict: the "
            "response shape can differ between pygbif versions.",
            file=sys.stderr,
        )
        return 1

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["species_key", "scientific_name", "common_name", "species_group"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote {len(rows)} species to {OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
