"""Request, monitor, and fetch a GBIF occurrence download.

Uses GBIF's download REST API directly (no pygbif), so it does not depend on a
particular pygbif version.

Usage, from the repo root (with credentials in .env):
    python -m pipelines.ingest_gbif request        # submit; prints a download key
    python -m pipelines.ingest_gbif status <key>   # check progress
    python -m pipelines.ingest_gbif fetch <key>    # wait, then download and unzip

Day 5 adds the manifest entry and the bronze Parquet conversion.
"""

import argparse
import csv
import os
import sys
import time
import zipfile
from pathlib import Path

import requests

API = "https://api.gbif.org/v1"
REPO_ROOT = Path(__file__).resolve().parents[1]
SEED = REPO_ROOT / "dbt" / "seeds" / "species_list.csv"
RAW_DIR = REPO_ROOT / "data" / "raw" / "gbif"

# Rough WA + OR box, counter-clockwise as GBIF requires. It deliberately over-covers
# (parts of ID, CA, BC); the exact study-area check happens later in dbt.
STUDY_AREA_WKT = "POLYGON((-124.9 41.9, -116.4 41.9, -116.4 49.1, -124.9 49.1, -124.9 41.9))"

FAILED_STATES = {"FAILED", "KILLED", "CANCELLED", "ERRORED"}


def load_species_keys(path: Path = SEED) -> list[int]:
    with path.open(newline="") as f:
        return [int(row["species_key"]) for row in csv.DictReader(f)]


def build_predicate(species_keys: list[int], geometry: str = STUDY_AREA_WKT) -> dict:
    """Filters applied by GBIF before the file is built (see README, cleaning rules)."""
    return {
        "type": "and",
        "predicates": [
            {"type": "in", "key": "TAXON_KEY", "values": [str(k) for k in species_keys]},
            {"type": "equals", "key": "HAS_COORDINATE", "value": "true"},
            {"type": "equals", "key": "HAS_GEOSPATIAL_ISSUE", "value": "false"},
            {"type": "equals", "key": "OCCURRENCE_STATUS", "value": "PRESENT"},
            {"type": "within", "geometry": geometry},
        ],
    }


def _credentials() -> tuple[str, str, str]:
    from dotenv import load_dotenv

    load_dotenv(REPO_ROOT / ".env")
    try:
        return os.environ["GBIF_USER"], os.environ["GBIF_PWD"], os.environ["GBIF_EMAIL"]
    except KeyError as e:
        raise SystemExit(f"Missing {e.args[0]} in .env (see .env.example)") from None


def request_download(species_keys: list[int] | None = None) -> str:
    """Submit a download request and return its key."""
    user, pwd, email = _credentials()
    keys = species_keys or load_species_keys()
    body = {
        "creator": user,
        "notificationAddresses": [email],
        "sendNotification": False,
        "format": "SIMPLE_CSV",
        "predicate": build_predicate(keys),
    }
    resp = requests.post(
        f"{API}/occurrence/download/request", json=body, auth=(user, pwd), timeout=60
    )
    if not resp.ok:
        raise SystemExit(f"GBIF rejected the request ({resp.status_code}): {resp.text[:500]}")
    return resp.text.strip().strip('"')


def get_status(key: str) -> dict:
    resp = requests.get(f"{API}/occurrence/download/{key}", timeout=30)
    resp.raise_for_status()
    return resp.json()


def wait_for_download(key: str, poll_seconds: int = 30, max_minutes: int = 120) -> dict:
    """Poll until the download succeeds. Raises if it fails or takes too long."""
    deadline = time.time() + max_minutes * 60
    while time.time() < deadline:
        meta = get_status(key)
        status = meta.get("status")
        print(f"  status: {status}")
        if status == "SUCCEEDED":
            return meta
        if status in FAILED_STATES:
            raise RuntimeError(f"Download {key} ended with status {status}")
        time.sleep(poll_seconds)
    raise TimeoutError(f"Download {key} not ready after {max_minutes} minutes")


def fetch_download(key: str, meta: dict) -> Path:
    """Download the zip, extract it under data/raw/gbif/<key>/, and return that folder."""
    out_dir = RAW_DIR / key
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / f"{key}.zip"
    link = meta.get("downloadLink") or f"{API}/occurrence/download/request/{key}.zip"

    with requests.get(link, stream=True, timeout=120) as r:
        r.raise_for_status()
        with zip_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)

    with zipfile.ZipFile(zip_path) as z:
        z.extractall(out_dir)
    return out_dir


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("request")
    sub.add_parser("status").add_argument("key")
    sub.add_parser("fetch").add_argument("key")
    args = parser.parse_args(argv)

    if args.cmd == "request":
        key = request_download()
        print(f"Download requested. Key: {key}")
        print("Save this key. Check progress with:")
        print(f"  python -m pipelines.ingest_gbif status {key}")
    elif args.cmd == "status":
        meta = get_status(args.key)
        for field in ("status", "doi", "totalRecords", "size", "downloadLink"):
            print(f"{field}: {meta.get(field)}")
    elif args.cmd == "fetch":
        meta = wait_for_download(args.key)
        print(f"DOI: {meta.get('doi')}  records: {meta.get('totalRecords')}")
        folder = fetch_download(args.key, meta)
        print(f"Extracted to {folder}")
        for p in sorted(folder.iterdir()):
            print(f"  {p.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
