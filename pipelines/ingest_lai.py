"""Summer leaf area index (LAI) from MODIS, paired with GBIF occurrence points.

Source: MODIS MOD15A2H (Terra, 8-day composites, 500 m), from Microsoft's Planetary Computer
(free, no account). LAI is the one-sided leaf area per unit ground area (m2/m2): roughly 0
for bare ground, 1-3 for open or shrubby cover, 5+ for dense conifer forest.

What "summer LAI" means here
    The mean of every good-quality July-August composite over the chosen years (default
    2019-2023). A multi-year summer average is deliberately used instead of matching each
    record's observation date: it measures the typical peak-season greenness of a place,
    and avoids dropping records for lack of a cloud-free image on their exact date.

Layers (same raw-first layout as the GBIF and WorldClim pipelines). Run from the repo root;
needs the `geo` extra, plus `ml` for the PNG:

    python -m pipelines.ingest_lai fetch [--years 2019 2023] [--refresh]
        bronze: save the raw LAI and quality-flag rasters, byte for byte, with a manifest.
        The only step that needs the network.
    python -m pipelines.ingest_lai build [--years 2019 2023]
        silver + gold: average the bronze scenes per tile, write the region map and summary.
        Reads only local files, so it can be rerun with a different year range or quality
        rule without downloading anything again.
    python -m pipelines.ingest_lai points
        silver: sample LAI at every occurrence in the bronze GBIF Parquet (for dbt).

Outputs (all under data/, which is git-ignored):
    data/bronze/modis_lai/<year>/<scene>_<asset>.tif     raw rasters, never modified
    data/bronze/modis_lai/manifest.csv                   scene, url, size, sha256
    data/silver/modis_lai/tiles/*.npz                    per-tile summer climatology
    data/silver/modis_lai_points/lai_points.parquet      one row per occurrence (for dbt)
    data/gold/lai/lai_summer_mean.tif|.png               region map (EPSG:4326, ~500 m)
    data/gold/lai/lai_summary.json                       region-wide statistics

Caveats that matter for interpretation
    * A 500 m pixel is a mix of everything inside it. LAI at a point describes the
      stand around it, not the tree itself.
    * MODIS publishes no LAI for water, urban, barren or snow/ice pixels, so occurrences
      in cities come back empty. That is a useful extra "is this a wild stand?" signal.
    * MODIS LAI saturates in very dense forest, so differences above about 5-6 are weak.
"""

import argparse
import csv
import hashlib
import json
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import requests

from pipelines.study_area import STUDY_BOX

REPO_ROOT = Path(__file__).resolve().parents[1]
STAC_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"
COLLECTION = "modis-15A2H-061"
LAI_ASSET = "Lai_500m"
QC_ASSET = "FparLai_QC"

LAI_SCALE = 0.1  # stored value x 0.1 = LAI in m2/m2
LAI_MAX_RAW = 100  # stored values above 100 are fill or land-cover codes (water, urban, ...)
DEFAULT_YEARS = (2019, 2023)  # inclusive
SUMMER = ("07-01", "08-31")  # July and August
GRID_RES_DEG = 0.005  # about 0.55 km north-south, 0.38 km east-west at 47 N
MAP_VMAX = 7.0  # color scale top; higher values share the darkest step

# MODIS sinusoidal grid
SINUSOIDAL = "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs"
TILE_RE = re.compile(r"h(\d{2})v(\d{2})")

BRONZE_DIR = REPO_ROOT / "data" / "bronze" / "modis_lai"
MANIFEST = BRONZE_DIR / "manifest.csv"
MANIFEST_COLUMNS = [
    "file",
    "scene",
    "tile",
    "asset",
    "datetime",
    "collection",
    "href",
    "bytes",
    "sha256",
    "downloaded_at",
]
HEADERS = {"User-Agent": "cascadia-flora/0.1"}
TILE_DIR = REPO_ROOT / "data" / "silver" / "modis_lai" / "tiles"
POINTS_OUT = REPO_ROOT / "data" / "silver" / "modis_lai_points" / "lai_points.parquet"
GOLD_DIR = REPO_ROOT / "data" / "gold" / "lai"
BRONZE_GLOB = REPO_ROOT / "data" / "bronze" / "gbif_occurrences" / "*.parquet"
STUDY_AREA_SEED = REPO_ROOT / "dbt" / "seeds" / "study_area.csv"

# Chart ink and surface (neutral tokens; data color comes from the Greens ramp)
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
NO_DATA = "#e1e0d9"


# ----------------------------------------------------------------------------------------
# Pure array logic (numpy only, so it is easy to test)
# ----------------------------------------------------------------------------------------


def good_quality(qc) -> np.ndarray:
    """True where the FparLai_QC byte says the retrieval is trustworthy.

    Bit layout, from the MOD15 user guide (verify against the guide if results look odd):
      bit 0     MODLAND_QC  0 = good (main algorithm)
      bits 3-4  CloudState  00 = clear
      bits 5-7  SCF_QC      000 = main method, best result; 001 = main method, saturated
                            (still good); 010+ = backup method or no retrieval
    """
    qc = np.asarray(qc, dtype=np.uint8)
    modland = qc & 0b1
    cloud = (qc >> 3) & 0b11
    scf = (qc >> 5) & 0b111
    return (modland == 0) & (cloud == 0) & (scf <= 1)


def lai_from_raw(raw, qc) -> np.ndarray:
    """Scale raw LAI bytes to m2/m2, with NaN wherever the value is fill or low quality."""
    raw = np.asarray(raw)
    valid = (raw <= LAI_MAX_RAW) & good_quality(qc)
    return np.where(valid, raw * LAI_SCALE, np.nan).astype(np.float32)


class Accumulator:
    """Running mean, peak, and count over many composites, ignoring NaN."""

    def __init__(self, shape):
        self.total = np.zeros(shape, dtype=np.float64)
        self.count = np.zeros(shape, dtype=np.int32)
        self.peak = np.full(shape, np.nan, dtype=np.float32)

    def add(self, lai) -> None:
        ok = ~np.isnan(lai)
        self.total[ok] += lai[ok]
        self.count[ok] += 1
        self.peak = np.fmax(self.peak, lai)  # fmax ignores NaN

    @property
    def mean(self) -> np.ndarray:
        with np.errstate(invalid="ignore", divide="ignore"):
            out = np.where(self.count > 0, self.total / np.maximum(self.count, 1), np.nan)
        return out.astype(np.float32)


def summarize(grid) -> dict:
    """Region-wide statistics for the mean-LAI grid."""
    vals = np.asarray(grid)[np.isfinite(grid)]
    if vals.size == 0:
        return {"valid_pixels": 0}
    bands = {
        "under_1_sparse": float(np.mean(vals < 1)),
        "1_to_3_open": float(np.mean((vals >= 1) & (vals < 3))),
        "3_to_5_moderate": float(np.mean((vals >= 3) & (vals < 5))),
        "5_and_up_dense": float(np.mean(vals >= 5)),
    }
    return {
        "valid_pixels": int(vals.size),
        "mean": round(float(vals.mean()), 2),
        "median": round(float(np.median(vals)), 2),
        "p10": round(float(np.percentile(vals, 10)), 2),
        "p90": round(float(np.percentile(vals, 90)), 2),
        "max": round(float(vals.max()), 2),
        "share_of_valid_pixels": {k: round(v, 3) for k, v in bands.items()},
    }


# ----------------------------------------------------------------------------------------
# Tiles: storage, point sampling, mosaic (need rasterio, imported lazily)
# ----------------------------------------------------------------------------------------


@dataclass
class Tile:
    name: str  # e.g. "h09v04"
    mean: np.ndarray
    peak: np.ndarray
    count: np.ndarray
    transform: object  # affine.Affine of the tile, in the sinusoidal CRS
    crs: str  # WKT


def save_tile(tile: Tile, folder: Path = TILE_DIR) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{tile.name}.npz"
    np.savez_compressed(
        path,
        mean=tile.mean,
        peak=tile.peak,
        count=tile.count,
        transform=np.array(tuple(tile.transform)[:6]),
        crs=np.array(tile.crs),
    )
    return path


def load_tiles(folder: Path = TILE_DIR) -> list[Tile]:
    from affine import Affine

    tiles = []
    for path in sorted(folder.glob("*.npz")):
        with np.load(path, allow_pickle=False) as d:
            tiles.append(
                Tile(
                    name=path.stem,
                    mean=d["mean"],
                    peak=d["peak"],
                    count=d["count"],
                    transform=Affine(*d["transform"]),
                    crs=str(d["crs"]),
                )
            )
    return tiles


def sample_tiles(tiles: list[Tile], lon, lat) -> dict:
    """Look up the pixel containing each point. Points outside every tile come back empty."""
    from rasterio.warp import transform as warp_transform

    lon = np.asarray(lon, dtype=float)
    lat = np.asarray(lat, dtype=float)
    n = len(lon)
    out = {
        "lai_summer_mean": np.full(n, np.nan, dtype=np.float32),
        "lai_summer_max": np.full(n, np.nan, dtype=np.float32),
        "lai_n_obs": np.zeros(n, dtype=np.int32),
        "modis_tile": np.full(n, None, dtype=object),
    }
    idx = np.flatnonzero(np.isfinite(lon) & np.isfinite(lat))
    if idx.size == 0:
        return out

    xs, ys = warp_transform("EPSG:4326", SINUSOIDAL, lon[idx].tolist(), lat[idx].tolist())
    xs, ys = np.asarray(xs), np.asarray(ys)

    for tile in tiles:
        height, width = tile.mean.shape
        inv = ~tile.transform  # written out: Affine's * operator for points is deprecated
        cols_f = inv.a * xs + inv.b * ys + inv.c
        rows_f = inv.d * xs + inv.e * ys + inv.f
        rows = np.floor(rows_f).astype(int)
        cols = np.floor(cols_f).astype(int)
        inside = (rows >= 0) & (rows < height) & (cols >= 0) & (cols < width)
        hit = idx[inside]
        r, c = rows[inside], cols[inside]
        out["lai_summer_mean"][hit] = tile.mean[r, c]
        out["lai_summer_max"][hit] = tile.peak[r, c]
        out["lai_n_obs"][hit] = tile.count[r, c]
        out["modis_tile"][hit] = tile.name
    return out


def mosaic_to_grid(tiles: list[Tile], bbox=STUDY_BOX, res=GRID_RES_DEG, field="mean"):
    """Reproject tiles onto one regular lon/lat grid covering bbox. Returns (grid, transform)."""
    from rasterio.transform import from_origin
    from rasterio.warp import Resampling, reproject

    west, south, east, north = bbox
    width = int(round((east - west) / res))
    height = int(round((north - south) / res))
    dst_transform = from_origin(west, north, res, res)

    grid = np.full((height, width), np.nan, dtype=np.float32)
    for tile in tiles:
        part = np.full((height, width), np.nan, dtype=np.float32)
        reproject(
            source=getattr(tile, field).astype(np.float32),
            destination=part,
            src_transform=tile.transform,
            src_crs=tile.crs,
            dst_transform=dst_transform,
            dst_crs="EPSG:4326",
            src_nodata=np.nan,
            dst_nodata=np.nan,
            resampling=Resampling.nearest,
        )
        grid = np.where(np.isnan(grid), part, grid)
    return grid, dst_transform


def study_area_mask(shape, transform):
    """Boolean grid, True inside the study-area polygon. None if it cannot be built."""
    try:
        import csv

        from rasterio.features import geometry_mask
        from shapely import wkt
    except ImportError:
        return None
    if not STUDY_AREA_SEED.exists():
        return None
    with STUDY_AREA_SEED.open(newline="") as f:
        geom = wkt.loads(next(csv.DictReader(f))["wkt"])
    return geometry_mask([geom], out_shape=shape, transform=transform, invert=True)


def write_geotiff(grid, transform, path: Path) -> None:
    import rasterio

    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=grid.shape[0],
        width=grid.shape[1],
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=np.nan,
        compress="deflate",
    ) as dst:
        dst.write(grid.astype(np.float32), 1)
        dst.set_band_description(1, "Summer (Jul-Aug) mean LAI, m2/m2")


def plot_map(grid, inside, path: Path, years: tuple[int, int]) -> bool:
    """Region map: one green ramp, light (sparse) to dark (dense). False if no matplotlib."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.colors import ListedColormap
    except ImportError:
        print("matplotlib not installed; skipping the PNG (pip install -e '.[ml]')")
        return False

    west, south, east, north = STUDY_BOX
    extent = (west, east, south, north)

    fig, ax = plt.subplots(figsize=(7.2, 8.4), dpi=160, facecolor=SURFACE)
    ax.set_facecolor(SURFACE)

    if inside is not None:  # land with no LAI (water, cities, rock, ice) in neutral gray
        gap = np.where(inside & np.isnan(grid), 1.0, np.nan)
        ax.imshow(gap, extent=extent, cmap=ListedColormap([NO_DATA]), interpolation="nearest")

    cmap = plt.get_cmap("Greens")  # one hue, monotone light to dark
    im = ax.imshow(grid, extent=extent, cmap=cmap, vmin=0, vmax=MAP_VMAX, interpolation="nearest")
    ax.set_aspect(1 / np.cos(np.radians((south + north) / 2)))

    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(INK_MUTED)
    ax.tick_params(colors=INK_MUTED, labelsize=8, length=3)
    ax.set_xlabel("Longitude", color=INK_SECONDARY, fontsize=9)
    ax.set_ylabel("Latitude", color=INK_SECONDARY, fontsize=9)

    cbar = fig.colorbar(im, ax=ax, shrink=0.55, pad=0.03, extend="max")
    cbar.set_label(
        "Summer leaf area index (m² leaf per m² ground)", color=INK_SECONDARY, fontsize=9
    )
    cbar.ax.tick_params(colors=INK_MUTED, labelsize=8)
    cbar.outline.set_visible(False)

    # Title and subtitle are tied to the map (not the figure), so they sit right above it
    # however the map's proportions change the figure size.
    ax.set_title(
        "Summer leaf area index across Cascadia",
        loc="left",
        color=INK,
        fontsize=14,
        fontweight="bold",
        pad=32,
    )
    ax.text(
        0,
        1.01,
        f"Mean of July-August MODIS composites, {years[0]}-{years[1]}, 500 m.\n"
        "Gray = land with no LAI (water, urban, bare rock, ice).",
        transform=ax.transAxes,
        color=INK_SECONDARY,
        fontsize=8.5,
        ha="left",
        va="bottom",
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    return True


# ----------------------------------------------------------------------------------------
# Bronze: find and download the raw scenes (the only code that touches the network)
# ----------------------------------------------------------------------------------------


@dataclass
class Scene:
    """One 8-day MODIS composite: where to fetch its LAI and quality-flag rasters."""

    id: str  # e.g. MOD15A2H.A2019185.h09v04.061.2020...
    tile: str  # e.g. "h09v04"
    year: int
    datetime: str
    hrefs: dict  # asset name -> URL


def list_scenes(years, client=None) -> list[Scene]:
    """Ask the Planetary Computer STAC catalog for the July-August composites over Cascadia."""
    if client is None:
        from pystac_client import Client

        client = Client.open(STAC_URL)

    scenes = []
    for year in years:
        search = client.search(
            collections=[COLLECTION],
            bbox=list(STUDY_BOX),
            datetime=f"{year}-{SUMMER[0]}/{year}-{SUMMER[1]}",
        )
        for item in search.items():
            m = TILE_RE.search(item.id)
            if not m:
                raise RuntimeError(f"Cannot read a tile name (hXXvYY) from item id {item.id}")
            missing = [a for a in (LAI_ASSET, QC_ASSET) if a not in item.assets]
            if missing:
                raise RuntimeError(
                    f"Item {item.id} has no asset(s) {missing}. "
                    f"Available: {sorted(item.assets)}. Fix LAI_ASSET / QC_ASSET."
                )
            when = item.datetime.isoformat() if item.datetime else ""
            hrefs = {a: item.assets[a].href for a in (LAI_ASSET, QC_ASSET)}
            scenes.append(Scene(item.id, f"h{m.group(1)}v{m.group(2)}", year, when, hrefs))
    return sorted(scenes, key=lambda s: s.id)


def scene_path(scene: Scene, asset: str, bronze_dir: Path = BRONZE_DIR) -> Path:
    return bronze_dir / str(scene.year) / f"{scene.id}_{asset}.tif"


def download_file(href: str, dest: Path, sign=None, tries: int = 3) -> tuple[int, str]:
    """Stream one raster to dest (via a .part file) and return (bytes, sha256).

    The URL is signed again on every attempt because Planetary Computer tokens expire.
    """
    if sign is None:
        import planetary_computer

        sign = planetary_computer.sign
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + ".part")
    last = None
    for attempt in range(tries):
        try:
            sha, size = hashlib.sha256(), 0
            with requests.get(sign(href), stream=True, timeout=(30, 120), headers=HEADERS) as r:
                r.raise_for_status()
                expected = int(r.headers.get("Content-Length", 0))
                with part.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
                        sha.update(chunk)
                        size += len(chunk)
            if expected and size != expected:
                raise OSError(f"got {size:,} of {expected:,} bytes")
            part.replace(dest)
            return size, sha.hexdigest()
        except (requests.RequestException, OSError) as exc:
            last = exc
            part.unlink(missing_ok=True)
            time.sleep(2**attempt)
    raise RuntimeError(f"Could not download {href}: {last}")


def _check_raster(path: Path) -> None:
    """Fail loudly if a downloaded file is not a readable one-band GeoTIFF."""
    import rasterio

    try:
        with rasterio.open(path) as src:
            ok = src.count == 1 and src.width > 0 and src.height > 0
    except rasterio.errors.RasterioError as exc:
        raise ValueError(f"{path} is not a readable raster: {exc}") from exc
    if not ok:
        raise ValueError(f"{path} does not look like a one-band MODIS tile")


def append_manifest(row: dict, manifest: Path = MANIFEST) -> None:
    manifest.parent.mkdir(parents=True, exist_ok=True)
    is_new = not manifest.exists()
    with manifest.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def fetch_scenes(
    scenes: list[Scene], bronze_dir: Path = BRONZE_DIR, sign=None, refresh: bool = False
) -> dict:
    """Save every scene's LAI and quality rasters into bronze. Files already there are kept."""
    new = skipped = total_bytes = 0
    for i, scene in enumerate(scenes, start=1):
        for asset in (LAI_ASSET, QC_ASSET):
            dest = scene_path(scene, asset, bronze_dir)
            if dest.exists() and not refresh:
                skipped += 1
                continue
            size, digest = download_file(scene.hrefs[asset], dest, sign=sign)
            try:
                _check_raster(dest)
            except ValueError:
                dest.unlink()
                raise
            append_manifest(
                {
                    "file": str(dest.relative_to(bronze_dir)),
                    "scene": scene.id,
                    "tile": scene.tile,
                    "asset": asset,
                    "datetime": scene.datetime,
                    "collection": COLLECTION,
                    "href": scene.hrefs[asset].split("?")[0],  # never store a signed token
                    "bytes": size,
                    "sha256": digest,
                    "downloaded_at": datetime.now(UTC).isoformat(timespec="seconds"),
                },
                bronze_dir / "manifest.csv",
            )
            new += 1
            total_bytes += size
        print(f"  {i}/{len(scenes)} scenes", end="\r", flush=True)
    print()
    return {"new_files": new, "skipped_files": skipped, "bytes": total_bytes}


# ----------------------------------------------------------------------------------------
# Silver: average the bronze scenes per tile (local files only)
# ----------------------------------------------------------------------------------------


@dataclass
class LocalScene:
    tile: str
    year: int
    lai_path: Path
    qc_path: Path


def find_local_scenes(years, bronze_dir: Path = BRONZE_DIR) -> list[LocalScene]:
    """Pair each bronze LAI raster with its quality raster, for the chosen years."""
    found = []
    for year in years:
        for lai in sorted((bronze_dir / str(year)).glob(f"*_{LAI_ASSET}.tif")):
            qc = lai.with_name(lai.name.replace(f"_{LAI_ASSET}.tif", f"_{QC_ASSET}.tif"))
            if not qc.exists():
                raise FileNotFoundError(f"{lai.name} has no matching quality file {qc.name}")
            m = TILE_RE.search(lai.name)
            if not m:
                raise ValueError(f"Cannot read a tile name from {lai.name}")
            found.append(LocalScene(f"h{m.group(1)}v{m.group(2)}", year, lai, qc))
    return found


def read_band(path: Path):
    """Band 1 of a local raster with its transform and CRS (as WKT)."""
    import rasterio

    with rasterio.open(path) as src:
        return src.read(1), src.transform, src.crs.to_wkt()


def build_tiles(scenes: list[LocalScene], tile_dir: Path = TILE_DIR) -> list[Tile]:
    """Average every scene of each tile into a summer climatology and save it."""
    if not scenes:
        raise FileNotFoundError(
            "No MODIS rasters found in bronze for those years. "
            "Run: python -m pipelines.ingest_lai fetch"
        )
    by_tile = defaultdict(list)
    for scene in scenes:
        by_tile[scene.tile].append(scene)

    for old in tile_dir.glob("*.npz"):  # tiles from an earlier year range would mislead
        old.unlink()

    tiles = []
    for name, group in sorted(by_tile.items()):
        acc, transform, crs = None, None, None
        for scene in group:
            raw, tf, wkt = read_band(scene.lai_path)
            qc, _, _ = read_band(scene.qc_path)
            if acc is None:
                acc, transform, crs = Accumulator(raw.shape), tf, wkt
            elif raw.shape != acc.count.shape or tf != transform:
                raise ValueError(f"{scene.lai_path.name} is on a different grid than the rest")
            acc.add(lai_from_raw(raw, qc))
        tile = Tile(name, acc.mean, acc.peak, acc.count, transform, crs)
        save_tile(tile, tile_dir)
        tiles.append(tile)
        print(
            f"  {name}: {len(group)} composites, "
            f"{int(np.isfinite(tile.mean).sum()):,} pixels with LAI"
        )
    return tiles


def build_points(tiles: list[Tile], bronze_glob=BRONZE_GLOB, out_path: Path = POINTS_OUT) -> dict:
    """Sample LAI at every bronze occurrence and write one row per gbif_id."""
    import duckdb

    quoted = str(bronze_glob).replace("'", "''")
    df = duckdb.sql(
        f"""
        SELECT gbif_id, lat, lon FROM (
            SELECT try_cast(gbifID AS BIGINT) AS gbif_id,
                   try_cast(decimalLatitude AS DOUBLE) AS lat,
                   try_cast(decimalLongitude AS DOUBLE) AS lon
            FROM read_parquet('{quoted}')
        )
        WHERE gbif_id IS NOT NULL AND lat IS NOT NULL AND lon IS NOT NULL
        """
    ).df()
    if not df["gbif_id"].is_unique:
        raise ValueError("Bronze occurrences contain duplicate gbifID values")

    sampled = sample_tiles(tiles, df["lon"].to_numpy(), df["lat"].to_numpy())
    df = df.drop(columns=["lat", "lon"])
    for col, values in sampled.items():
        df[col] = values

    out_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.register("points", df)
    con.execute(f"COPY points TO '{str(out_path).replace(chr(39), chr(39) * 2)}' (FORMAT parquet)")
    con.close()
    have = int(df["lai_summer_mean"].notna().sum())
    return {"rows": len(df), "with_lai": have, "out_path": out_path}


# ----------------------------------------------------------------------------------------
# Commands
# ----------------------------------------------------------------------------------------


def cmd_fetch(args) -> int:
    years = range(args.years[0], args.years[1] + 1)
    scenes = list_scenes(years)
    if not scenes:
        print("The STAC search returned no MODIS scenes. Check the years and network access.")
        return 1
    tiles = sorted({s.tile for s in scenes})
    print(f"Found {len(scenes)} composites in {len(tiles)} tiles: {tiles}")
    stats = fetch_scenes(scenes, refresh=args.refresh)
    print(
        f"Saved {stats['new_files']} new files ({stats['bytes'] / 1e6:,.0f} MB), "
        f"{stats['skipped_files']} already in bronze. Manifest: {MANIFEST}"
    )
    return 0


def cmd_build(args) -> int:
    years = tuple(args.years)
    try:
        scenes = find_local_scenes(range(years[0], years[1] + 1))
        tiles = build_tiles(scenes)
    except FileNotFoundError as exc:
        print(exc, file=sys.stderr)
        return 1
    years_used = sorted({s.year for s in scenes})

    grid, transform = mosaic_to_grid(tiles)
    inside = study_area_mask(grid.shape, transform)
    if inside is None:
        print("Study-area mask unavailable (needs shapely and dbt/seeds/study_area.csv).")
    else:
        grid = np.where(inside, grid, np.nan).astype(np.float32)

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    write_geotiff(grid, transform, GOLD_DIR / "lai_summer_mean.tif")
    plot_map(grid, inside, GOLD_DIR / "lai_summer_mean.png", (years_used[0], years_used[-1]))

    summary = {
        "source": f"MODIS MOD15A2H via Planetary Computer ({COLLECTION}), band {LAI_ASSET}",
        "definition": "mean of good-quality July-August 8-day composites",
        "years_requested": list(years),
        "years_found_in_bronze": years_used,
        "composites_used": len(scenes),
        "quality_rule": "MODLAND_QC=0, CloudState=clear, SCF_QC<=1; water/urban/barren excluded",
        "grid_degrees": GRID_RES_DEG,
        **summarize(grid),
    }
    if inside is not None:
        summary["share_of_study_area_with_lai"] = round(float(np.isfinite(grid[inside]).mean()), 3)
    (GOLD_DIR / "lai_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    print(f"Wrote outputs to {GOLD_DIR}")
    return 0


def cmd_points(args) -> int:
    tiles = load_tiles()
    if not tiles:
        print("No tiles found. Run: python -m pipelines.ingest_lai build", file=sys.stderr)
        return 1
    stats = build_points(tiles)
    print(f"Wrote {stats['rows']:,} rows to {stats['out_path']}")
    print(
        f"LAI found for {stats['with_lai']:,} ({stats['with_lai'] / max(stats['rows'], 1):.1%}); "
        "the rest are water, urban, bare, outside the tiles, or have no coordinates."
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)

    years_kw = {
        "nargs": 2,
        "type": int,
        "default": list(DEFAULT_YEARS),
        "metavar": ("FIRST", "LAST"),
    }

    fetch = sub.add_parser("fetch", help="download raw MODIS rasters into bronze")
    fetch.add_argument("--years", **years_kw)
    fetch.add_argument("--refresh", action="store_true", help="download again even if present")
    fetch.set_defaults(func=cmd_fetch)

    build = sub.add_parser("build", help="average the bronze scenes and write the region map")
    build.add_argument("--years", **years_kw)
    build.set_defaults(func=cmd_build)

    points = sub.add_parser("points", help="sample LAI at each GBIF occurrence")
    points.set_defaults(func=cmd_points)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
