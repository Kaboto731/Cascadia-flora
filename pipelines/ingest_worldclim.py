"""WorldClim 2.1 historical climate (1970-2000): bronze download, study-area clip, point sampling.

WorldClim's 19 bioclimatic variables are the climate features for the species models.
The layers are 1 km at the default resolution ("30s"), global, and about a gigabyte as a
zip, so the work is split the same way as the rest of the pipeline:

    python -m pipelines.ingest_worldclim download        # bronze: the raw zip, untouched
    python -m pipelines.ingest_worldclim clip            # silver: 19-band GeoTIFF of Cascadia
    python -m pipelines.ingest_worldclim points          # silver: climate at every occurrence

Use `--res 10m` on any step to try the whole thing on a small, coarse copy first
(resolutions: 10m, 5m, 2.5m, 30s; the choice is part of every output file name).

Produces:
    data/bronze/worldclim/wc2.1_<res>_bio.zip                    (raw, as downloaded)
    data/bronze/worldclim/manifest.csv                           (url, date, size, sha256)
    data/silver/worldclim/bio_<res>_cascadia.tif                 (19 bands, float32, NaN = none)
    data/silver/worldclim_points/worldclim_points.parquet        (gbif_id + bio_1..bio_19)

dbt joins the points file onto the cleaned occurrences (int_occurrences__climate).

Units (WorldClim 2.1): temperatures are in degrees C (not x10), precipitation in mm.
"""

import argparse
import csv
import hashlib
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import numpy as np
import requests

from pipelines.ingest_gbif import REPO_ROOT
from pipelines.study_area import STUDY_BOX

BASE_URL = "https://geodata.ucdavis.edu/climate/worldclim/2_1/base"
RESOLUTIONS = ("10m", "5m", "2.5m", "30s")
DEFAULT_RES = "30s"
N_BIO = 19
HEADERS = {"User-Agent": "cascadia-flora/0.1"}

BRONZE_DIR = REPO_ROOT / "data" / "bronze" / "worldclim"
MANIFEST = BRONZE_DIR / "manifest.csv"
SILVER_DIR = REPO_ROOT / "data" / "silver" / "worldclim"
POINTS_OUT = REPO_ROOT / "data" / "silver" / "worldclim_points" / "worldclim_points.parquet"
BRONZE_GBIF_GLOB = REPO_ROOT / "data" / "bronze" / "gbif_occurrences" / "*.parquet"
MANIFEST_COLUMNS = ["file", "url", "resolution", "downloaded_at", "bytes", "sha256"]

BIO_NAMES = {
    1: "Annual mean temperature (C)",
    2: "Mean diurnal range (C)",
    3: "Isothermality (bio2 / bio7, x100)",
    4: "Temperature seasonality (standard deviation x100)",
    5: "Max temperature of warmest month (C)",
    6: "Min temperature of coldest month (C)",
    7: "Temperature annual range (C)",
    8: "Mean temperature of wettest quarter (C)",
    9: "Mean temperature of driest quarter (C)",
    10: "Mean temperature of warmest quarter (C)",
    11: "Mean temperature of coldest quarter (C)",
    12: "Annual precipitation (mm)",
    13: "Precipitation of wettest month (mm)",
    14: "Precipitation of driest month (mm)",
    15: "Precipitation seasonality (coefficient of variation)",
    16: "Precipitation of wettest quarter (mm)",
    17: "Precipitation of driest quarter (mm)",
    18: "Precipitation of warmest quarter (mm)",
    19: "Precipitation of coldest quarter (mm)",
}


# --------------------------------------------------------------------------- names and paths


def zip_name(res: str) -> str:
    return f"wc2.1_{res}_bio.zip"


def zip_url(res: str) -> str:
    return f"{BASE_URL}/{zip_name(res)}"


def member_name(res: str, n: int) -> str:
    """Name of one band's file inside the WorldClim zip."""
    return f"wc2.1_{res}_bio_{n}.tif"


def stack_file(res: str) -> Path:
    return SILVER_DIR / f"bio_{res}_cascadia.tif"


def _q(value: Path | str) -> str:
    """Escape a path for use inside a single-quoted SQL string."""
    return str(value).replace("'", "''")


# --------------------------------------------------------------------------- bronze download


def check_zip(path: Path, res: str) -> None:
    """Raise ValueError unless the file is a zip holding all 19 expected bands."""
    try:
        with zipfile.ZipFile(path) as zf:
            names = set(zf.namelist())
    except zipfile.BadZipFile as exc:
        raise ValueError(f"{path} is not a valid zip file (incomplete download?)") from exc
    missing = [member_name(res, n) for n in range(1, N_BIO + 1) if member_name(res, n) not in names]
    if missing:
        raise ValueError(f"{path} is missing bands: {missing}")


def append_manifest(row: dict, manifest: Path = MANIFEST) -> None:
    manifest.parent.mkdir(parents=True, exist_ok=True)
    is_new = not manifest.exists()
    with manifest.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def download(res: str = DEFAULT_RES, refresh: bool = False, dest_dir: Path = BRONZE_DIR) -> Path:
    """Save the raw WorldClim zip into bronze and record its checksum. Never modifies it."""
    url, dest = zip_url(res), dest_dir / zip_name(res)
    if dest.exists() and not refresh:
        check_zip(dest, res)
        print(f"Already downloaded: {dest} (use --refresh to fetch again)")
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    sha, size, next_report = hashlib.sha256(), 0, 100e6
    print(f"Downloading {url}")
    with requests.get(url, stream=True, timeout=(30, 120), headers=HEADERS) as resp:
        resp.raise_for_status()
        expected = int(resp.headers.get("Content-Length", 0))
        with part.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                sha.update(chunk)
                size += len(chunk)
                if size >= next_report:
                    print(f"  {size / 1e6:,.0f} MB", flush=True)
                    next_report += 100e6
    if expected and size != expected:
        part.unlink()
        raise OSError(f"Download cut short: got {size:,} of {expected:,} bytes. Run it again.")
    try:
        check_zip(part, res)
    except ValueError:
        part.unlink()
        raise
    part.replace(dest)
    append_manifest(
        {
            "file": dest.name,
            "url": url,
            "resolution": res,
            "downloaded_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "bytes": size,
            "sha256": sha.hexdigest(),
        },
        dest_dir / "manifest.csv",
    )
    print(f"Saved {dest} ({size / 1e6:,.0f} MB)")
    return dest


# --------------------------------------------------------------------------- silver: clip


def _bbox_window(src, bbox):
    """Pixel window of a raster covering bbox, clamped to the raster's own extent."""
    from rasterio.windows import Window

    west, south, east, north = bbox
    row0, col0 = src.index(west, north)
    row1, col1 = src.index(east, south)
    row0, col0 = max(row0, 0), max(col0, 0)
    row1, col1 = min(row1, src.height - 1), min(col1, src.width - 1)
    if row1 < row0 or col1 < col0:
        raise ValueError(f"bbox {bbox} lies outside the raster")
    return Window(col0, row0, col1 - col0 + 1, row1 - row0 + 1)


def clip_to_bbox(zip_path: Path, res: str, out_path: Path, bbox=STUDY_BOX) -> Path:
    """Cut the 19 bands to bbox and stack them into one compressed GeoTIFF (NaN = no data)."""
    import rasterio

    check_zip(zip_path, res)
    bands, transform, crs = [], None, None
    for n in range(1, N_BIO + 1):
        with rasterio.open(f"/vsizip/{Path(zip_path).as_posix()}/{member_name(res, n)}") as src:
            window = _bbox_window(src, bbox)
            data = src.read(1, window=window).astype("float32")
            if src.nodata is not None:
                data[data == src.nodata] = np.nan
            data[data < -1e30] = np.nan  # float32 "min" fill values
            band_transform = src.window_transform(window)
            if transform is None:
                transform, crs = band_transform, src.crs
            elif band_transform != transform or data.shape != bands[0].shape:
                raise ValueError(f"Band {n} is on a different grid than band 1")
            bands.append(data)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    height, width = bands[0].shape
    with rasterio.open(
        out_path,
        "w",
        driver="GTiff",
        dtype="float32",
        count=N_BIO,
        height=height,
        width=width,
        crs=crs,
        transform=transform,
        nodata=np.nan,
        compress="deflate",
        tiled=True,
        blockxsize=256,
        blockysize=256,
    ) as dst:
        for n, data in enumerate(bands, start=1):
            dst.write(data, n)
            dst.set_band_description(n, f"bio_{n}")
        dst.update_tags(source="WorldClim 2.1 (1970-2000)", resolution=res)
    print(f"Wrote {out_path} ({width} x {height} pixels, {N_BIO} bands)")
    return out_path


# --------------------------------------------------------------------------- silver: points


def sample_stack(stack_path: Path, lon, lat, snap: int = 1):
    """Read the climate values at each lon/lat.

    Returns (values, snapped): values has one row per point and one column per band, NaN
    where there is no climate (open water, outside the raster). WorldClim only covers land,
    so a coastal point can fall on a no-data pixel. With snap > 0 such a point takes the
    values of the nearest land pixel within `snap` pixels (1 pixel is about 1 km at 30s,
    inside the 1 km coordinate-uncertainty limit) and is reported in `snapped`.
    """
    import rasterio

    with rasterio.open(stack_path) as src:
        stack = src.read()
        transform = src.transform
    n_bands, height, width = stack.shape

    lon, lat = np.asarray(lon, dtype=float), np.asarray(lat, dtype=float)
    inv = ~transform  # lon/lat -> fractional column/row (written out: Affine operators changed)
    cols_f = inv.a * lon + inv.b * lat + inv.c
    rows_f = inv.d * lon + inv.e * lat + inv.f
    cols, rows = np.floor(cols_f).astype(int), np.floor(rows_f).astype(int)
    inside = (rows >= 0) & (rows < height) & (cols >= 0) & (cols < width)

    values = np.full((len(lon), n_bands), np.nan, dtype="float32")
    values[inside] = stack[:, rows[inside], cols[inside]].T
    snapped = np.zeros(len(lon), dtype=bool)

    if snap > 0:
        valid = ~np.isnan(stack[0])
        for i in np.flatnonzero(inside & np.isnan(values[:, 0])):
            r0, r1 = max(rows[i] - snap, 0), min(rows[i] + snap + 1, height)
            c0, c1 = max(cols[i] - snap, 0), min(cols[i] + snap + 1, width)
            found = np.argwhere(valid[r0:r1, c0:c1])
            if len(found) == 0:
                continue
            rr, cc = found[:, 0] + r0, found[:, 1] + c0
            nearest = ((rr + 0.5 - rows_f[i]) ** 2 + (cc + 0.5 - cols_f[i]) ** 2).argmin()
            values[i] = stack[:, rr[nearest], cc[nearest]]
            snapped[i] = True
    return values, snapped


def load_occurrence_coords(bronze_glob: Path | str = BRONZE_GBIF_GLOB):
    """gbif_id, lat, lon for every bronze record that has usable coordinates."""
    query = f"""
        SELECT gbif_id, lat, lon FROM (
            SELECT try_cast(gbifID AS bigint)            AS gbif_id,
                   try_cast(decimalLatitude AS double)   AS lat,
                   try_cast(decimalLongitude AS double)  AS lon
            FROM read_parquet('{_q(bronze_glob)}')
        )
        WHERE gbif_id IS NOT NULL AND lat IS NOT NULL AND lon IS NOT NULL
    """
    return duckdb.sql(query).df()


def build_points(
    res: str = DEFAULT_RES,
    bronze_glob: Path | str = BRONZE_GBIF_GLOB,
    stack_path: Path | None = None,
    out_path: Path = POINTS_OUT,
    snap: int = 1,
) -> dict:
    """Sample climate at every bronze occurrence and write one row per gbif_id."""
    import pandas as pd

    stack_path = stack_path or stack_file(res)
    coords = load_occurrence_coords(bronze_glob)
    if not coords["gbif_id"].is_unique:
        raise ValueError("Bronze occurrences contain duplicate gbifID values")

    values, snapped = sample_stack(stack_path, coords["lon"], coords["lat"], snap=snap)
    out = pd.DataFrame(values, columns=[f"bio_{n}" for n in range(1, N_BIO + 1)])
    out.insert(0, "gbif_id", coords["gbif_id"].to_numpy())
    out["climate_snapped"] = snapped
    out["worldclim_res"] = res

    out_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.register("points", out)
    con.execute(f"COPY points TO '{_q(out_path)}' (FORMAT parquet)")
    con.close()
    return {
        "points": len(out),
        "with_climate": int(out["bio_1"].notna().sum()),
        "snapped": int(snapped.sum()),
        "no_climate": int(out["bio_1"].isna().sum()),
        "out_path": out_path,
    }


# --------------------------------------------------------------------------- command line


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    for name, helptext in [
        ("download", "save the raw zip into bronze"),
        ("clip", "cut the study area out of the bronze zip"),
        ("points", "sample climate at every occurrence"),
    ]:
        p = sub.add_parser(name, help=helptext)
        p.add_argument("--res", choices=RESOLUTIONS, default=DEFAULT_RES)
    sub.choices["download"].add_argument("--refresh", action="store_true")
    sub.choices["points"].add_argument(
        "--snap", type=int, default=1, help="pixels to look for the nearest land (0 = off)"
    )
    args = parser.parse_args(argv)

    if args.command == "download":
        download(args.res, refresh=args.refresh)
        return 0

    if args.command == "clip":
        zip_path = BRONZE_DIR / zip_name(args.res)
        if not zip_path.exists():
            print(f"{zip_path} not found. Run: python -m pipelines.ingest_worldclim download")
            return 1
        clip_to_bbox(zip_path, args.res, stack_file(args.res))
        return 0

    stack = stack_file(args.res)
    if not stack.exists():
        zip_path = BRONZE_DIR / zip_name(args.res)
        if not zip_path.exists():
            print(f"{zip_path} not found. Run: python -m pipelines.ingest_worldclim download")
            return 1
        clip_to_bbox(zip_path, args.res, stack)
    stats = build_points(args.res, snap=args.snap)
    print(f"Wrote {stats['out_path']}")
    print(f"  occurrences sampled : {stats['points']:,}")
    print(f"  with climate        : {stats['with_climate']:,}")
    print(f"    of which snapped to nearest land pixel: {stats['snapped']:,}")
    print(f"  no climate (water / outside raster): {stats['no_climate']:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
