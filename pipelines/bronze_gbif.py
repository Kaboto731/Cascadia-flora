"""Load a fetched GBIF download into bronze Parquet, record it, and write baseline stats.

Run from the repo root after `python -m pipelines.ingest_gbif fetch <key>`:
    python -m pipelines.bronze_gbif <key>

Produces:
    data/bronze/gbif_occurrences/occurrences.parquet   (all columns kept as text)
    dbt/seeds/gbif_download_manifest.csv               (one row per download; commit this)
    docs/week1_notes.md                                (before-cleaning baseline stats)
"""

import argparse
import csv
import json
import sys
from pathlib import Path

import duckdb

from pipelines.ingest_gbif import RAW_DIR, REPO_ROOT, get_status, load_species_keys

BRONZE_FILE = REPO_ROOT / "data" / "bronze" / "gbif_occurrences" / "occurrences.parquet"
MANIFEST = REPO_ROOT / "dbt" / "seeds" / "gbif_download_manifest.csv"
NOTES = REPO_ROOT / "docs" / "week1_notes.md"
MANIFEST_COLUMNS = [
    "download_key",
    "doi",
    "created_date",
    "filters",
    "record_count",
    "species_keys",
]


def _q(path: Path) -> str:
    """Escape a path for use inside a single-quoted SQL string."""
    return str(path).replace("'", "''")


def find_raw_file(folder: Path) -> Path:
    """Find the tab-delimited occurrence file inside an extracted download."""
    candidates = [p for p in folder.iterdir() if p.suffix in {".csv", ".txt", ".tsv"}]
    if not candidates:
        raise FileNotFoundError(f"No .csv/.txt file found in {folder}")
    return max(candidates, key=lambda p: p.stat().st_size)


def csv_to_parquet(src: Path, dest: Path) -> int:
    """Convert the raw file to Parquet and return the row count.

    Everything stays text (all_varchar) so bronze is a faithful copy of what GBIF sent;
    typing happens in dbt staging. ignore_errors is off on purpose: a malformed row
    should fail loudly instead of being silently dropped.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(
        f"""
        COPY (
            SELECT * FROM read_csv('{_q(src)}', delim='\\t', quote='', header=true,
                                   all_varchar=true, ignore_errors=false)
        ) TO '{_q(dest)}' (FORMAT parquet)
        """
    )
    # fetchall() reads the result to the end. Newer DuckDB keeps a connection busy after
    # fetchone(), which broke the next query on the shared default connection.
    rows = duckdb.sql(f"SELECT count(*) FROM read_parquet('{_q(dest)}')").fetchall()
    return rows[0][0]


def append_manifest(row: dict, path: Path = MANIFEST) -> bool:
    """Append a manifest row unless that download key is already recorded."""
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = []
    if path.exists():
        with path.open(newline="") as f:
            existing = list(csv.DictReader(f))
    if any(r["download_key"] == row["download_key"] for r in existing):
        return False
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)
    return True


def _md_table(headers: list[str], rows: list[tuple]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows]
    return "\n".join(lines)


def baseline_stats(parquet: Path) -> str:
    """Before-cleaning numbers, as markdown. These anchor the audit story later."""
    src = f"read_parquet('{_q(parquet)}')"

    def run(sql: str) -> list[tuple]:
        return duckdb.sql(sql.replace("{src}", src)).fetchall()

    sections = []

    total = run("SELECT count(*) FROM {src}")[0][0]
    sections.append(f"**Total records:** {total:,}")

    sections.append(
        "### Records per species\n"
        + _md_table(
            ["species", "records"],
            run("SELECT species, count(*) n FROM {src} GROUP BY 1 ORDER BY n DESC"),
        )
    )

    sections.append(
        "### Records per decade\n"
        + _md_table(
            ["decade", "records"],
            run(
                "SELECT try_cast(year AS INTEGER) // 10 * 10 AS decade, count(*) "
                "FROM {src} GROUP BY 1 ORDER BY 1"
            ),
        )
    )

    sections.append(
        "### Missing coordinate uncertainty (% of records, per species)\n"
        + _md_table(
            ["species", "% null uncertainty"],
            run(
                "SELECT species, round(100.0 * avg((coordinateUncertaintyInMeters IS NULL)"
                "::INTEGER), 1) FROM {src} GROUP BY 1 ORDER BY 1"
            ),
        )
    )

    sections.append(
        "### Basis of record\n"
        + _md_table(
            ["basisOfRecord", "records"],
            run("SELECT basisOfRecord, count(*) n FROM {src} GROUP BY 1 ORDER BY n DESC"),
        )
    )

    sections.append(
        "### Country and state/province (top 20)\n"
        + _md_table(
            ["countryCode", "stateProvince", "records"],
            run(
                "SELECT countryCode, stateProvince, count(*) n FROM {src} "
                "GROUP BY 1, 2 ORDER BY n DESC LIMIT 20"
            ),
        )
    )

    sections.append(
        "### Coordinate extent (sanity check)\n"
        + _md_table(
            ["min lat", "max lat", "min lon", "max lon"],
            run(
                "SELECT min(try_cast(decimalLatitude AS DOUBLE)), "
                "max(try_cast(decimalLatitude AS DOUBLE)), "
                "min(try_cast(decimalLongitude AS DOUBLE)), "
                "max(try_cast(decimalLongitude AS DOUBLE)) FROM {src}"
            ),
        )
    )

    return "\n\n".join(sections)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("key", help="GBIF download key")
    args = parser.parse_args(argv)

    folder = RAW_DIR / args.key
    if not folder.exists():
        print(f"{folder} not found. Run: python -m pipelines.ingest_gbif fetch {args.key}")
        return 1

    meta = get_status(args.key)
    src = find_raw_file(folder)
    print(f"Converting {src.name} ...")
    rows = csv_to_parquet(src, BRONZE_FILE)
    print(f"Wrote {rows:,} rows to {BRONZE_FILE}")

    expected = meta.get("totalRecords")
    if expected is not None and rows != int(expected):
        print(
            f"WARNING: Parquet has {rows:,} rows but GBIF reports {int(expected):,}. "
            "Investigate before trusting this load.",
            file=sys.stderr,
        )
        return 1

    predicate = (meta.get("request") or {}).get("predicate")
    added = append_manifest(
        {
            "download_key": args.key,
            "doi": meta.get("doi", ""),
            "created_date": str(meta.get("created", ""))[:10],
            "filters": json.dumps(predicate, separators=(",", ":")) if predicate else "",
            "record_count": rows,
            "species_keys": ";".join(str(k) for k in load_species_keys()),
        }
    )
    print("Manifest row added." if added else "Manifest already has this download key.")

    NOTES.parent.mkdir(parents=True, exist_ok=True)
    header = (
        "# Week 1 notes: baseline stats (before cleaning)\n\n"
        f"GBIF download `{args.key}`, DOI `{meta.get('doi')}`. "
        "Generated by `pipelines/bronze_gbif.py`.\n\n"
    )
    NOTES.write_text(header + baseline_stats(BRONZE_FILE) + "\n")
    print(f"Wrote {NOTES}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
