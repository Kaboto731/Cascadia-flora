"""Offline tests for the MODIS LAI pipeline, using small synthetic tiles.

The synthetic tile is 60 x 60 pixels of 463.3 m (a real MODIS pixel) in the MODIS sinusoidal
projection, placed so the middle of Seattle is exactly the middle of pixel row 30, column 30.
Two composites are written:

    composite A: raw LAI 30 everywhere (LAI 3.0), all good quality
    composite B: raw LAI 50 everywhere (LAI 5.0), except
        (30, 30)  cloudy -> must be ignored, so that pixel's mean is 3.0 from one composite
        (30, 31)  raw 250 (urban fill code) in both composites -> no LAI
    so pixel (30, 32) is the mean of 3.0 and 5.0 = 4.0 from two composites.
"""

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import duckdb
import numpy as np
import pytest

from pipelines import ingest_lai as lai

PIXEL = 463.3127165275  # metres, MODIS 500 m sinusoidal pixel
SIZE = 60
TILE = "h09v04"
SEATTLE = (-122.33, 47.61)  # lon, lat
CLOUDY = 0b01000  # FparLai_QC: CloudState = 01 (cloudy)


# ----------------------------------------------------------------------- pure numpy logic


def test_good_quality_reads_the_qc_bits():
    qc = np.array(
        [
            0b00000000,  # everything best
            0b00000001,  # MODLAND_QC set: not produced by the main algorithm
            0b00001000,  # cloudy
            0b00010000,  # cloud state 10
            0b00100000,  # SCF_QC = 001: main method, saturated -> still good
            0b01000000,  # SCF_QC = 010: backup method -> not good
        ],
        dtype=np.uint8,
    )
    assert lai.good_quality(qc).tolist() == [True, False, False, False, True, False]


def test_lai_from_raw_scales_and_masks():
    raw = np.array([0, 25, 100, 101, 250, 25], dtype=np.uint8)
    qc = np.array([0, 0, 0, 0, 0, CLOUDY], dtype=np.uint8)
    out = lai.lai_from_raw(raw, qc)
    assert out[:3].tolist() == pytest.approx([0.0, 2.5, 10.0])
    assert np.isnan(out[3:]).all()  # above 100 is fill / land-cover code; last one is cloudy


def test_accumulator_mean_peak_and_count_ignore_nan():
    acc = lai.Accumulator((1, 3))
    acc.add(np.array([[3.0, np.nan, 2.0]], dtype=np.float32))
    acc.add(np.array([[5.0, np.nan, np.nan]], dtype=np.float32))
    assert acc.mean[0, 0] == 4.0 and acc.peak[0, 0] == 5.0 and acc.count[0, 0] == 2
    assert np.isnan(acc.mean[0, 1]) and np.isnan(acc.peak[0, 1]) and acc.count[0, 1] == 0
    assert acc.mean[0, 2] == 2.0 and acc.count[0, 2] == 1


def test_summarize_bands_and_empty_grid():
    grid = np.array([[0.5, 2.0, 4.0, 6.0, np.nan]], dtype=np.float32)
    s = lai.summarize(grid)
    assert s["valid_pixels"] == 4
    assert s["share_of_valid_pixels"] == {
        "under_1_sparse": 0.25,
        "1_to_3_open": 0.25,
        "3_to_5_moderate": 0.25,
        "5_and_up_dense": 0.25,
    }
    assert lai.summarize(np.full((2, 2), np.nan)) == {"valid_pixels": 0}


# ----------------------------------------------------------------------- synthetic bronze


def _seattle_xy():
    from rasterio.warp import transform

    xs, ys = transform("EPSG:4326", lai.SINUSOIDAL, [SEATTLE[0]], [SEATTLE[1]])
    return xs[0], ys[0]


def _tile_transform():
    from affine import Affine

    sx, sy = _seattle_xy()
    # Seattle in the middle of row 30, column 30
    return Affine(PIXEL, 0, sx - 30.5 * PIXEL, 0, -PIXEL, sy + 30.5 * PIXEL)


def _pixel_lonlat(row, col):
    """lon, lat of the middle of a pixel in the synthetic tile."""
    from rasterio.warp import transform

    t = _tile_transform()
    x = t.a * (col + 0.5) + t.c
    y = t.e * (row + 0.5) + t.f
    lons, lats = transform(lai.SINUSOIDAL, "EPSG:4326", [x], [y])
    return lons[0], lats[0]


def _write_band(path: Path, data):
    import rasterio

    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        dtype="uint8",
        count=1,
        height=SIZE,
        width=SIZE,
        crs=lai.SINUSOIDAL,
        transform=_tile_transform(),
    ) as dst:
        dst.write(data.astype("uint8"), 1)


def _write_scene(bronze: Path, year: int, scene_id: str, raw, qc):
    _write_band(bronze / str(year) / f"{scene_id}_{lai.LAI_ASSET}.tif", raw)
    _write_band(bronze / str(year) / f"{scene_id}_{lai.QC_ASSET}.tif", qc)


@pytest.fixture(scope="module")
def bronze(tmp_path_factory) -> Path:
    pytest.importorskip("rasterio")
    root = tmp_path_factory.mktemp("bronze_modis")

    raw_a = np.full((SIZE, SIZE), 30)
    raw_a[30, 31] = 250
    _write_scene(root, 2020, f"MOD15A2H.A2020185.{TILE}.061.a", raw_a, np.zeros((SIZE, SIZE)))

    raw_b = np.full((SIZE, SIZE), 50)
    raw_b[30, 31] = 250
    qc_b = np.zeros((SIZE, SIZE))
    qc_b[30, 30] = CLOUDY
    _write_scene(root, 2021, f"MOD15A2H.A2021185.{TILE}.061.b", raw_b, qc_b)
    return root


@pytest.fixture(scope="module")
def tiles(bronze, tmp_path_factory):
    scenes = lai.find_local_scenes(range(2020, 2022), bronze)
    return lai.build_tiles(scenes, tmp_path_factory.mktemp("tiles"))


# ----------------------------------------------------------------------- silver


def test_find_local_scenes_filters_years_and_pairs_files(bronze):
    both = lai.find_local_scenes(range(2020, 2022), bronze)
    assert [(s.tile, s.year) for s in both] == [(TILE, 2020), (TILE, 2021)]
    only_2021 = lai.find_local_scenes([2021], bronze)
    assert len(only_2021) == 1 and only_2021[0].lai_path.parent.name == "2021"
    assert lai.find_local_scenes([2019], bronze) == []


def test_missing_quality_file_is_an_error(tmp_path):
    pytest.importorskip("rasterio")
    _write_band(
        tmp_path / "2020" / f"MOD15A2H.A2020185.{TILE}.061.x_{lai.LAI_ASSET}.tif",
        np.zeros((SIZE, SIZE)),
    )
    with pytest.raises(FileNotFoundError, match="quality"):
        lai.find_local_scenes([2020], tmp_path)


def test_build_tiles_with_nothing_in_bronze_says_to_fetch(tmp_path):
    with pytest.raises(FileNotFoundError, match="fetch"):
        lai.build_tiles([], tmp_path)


def test_tile_averages_ignore_cloudy_and_fill_pixels(tiles):
    (tile,) = tiles
    assert tile.name == TILE
    assert tile.mean[30, 30] == pytest.approx(3.0)  # the cloudy composite is excluded
    assert tile.count[30, 30] == 1
    assert tile.mean[30, 32] == pytest.approx(4.0)  # mean of 3.0 and 5.0
    assert tile.peak[30, 32] == pytest.approx(5.0)
    assert tile.count[30, 32] == 2
    assert np.isnan(tile.mean[30, 31]) and tile.count[30, 31] == 0  # urban code, both scenes


def test_tiles_round_trip_through_disk(tiles, tmp_path):
    lai.save_tile(tiles[0], tmp_path)
    (back,) = lai.load_tiles(tmp_path)
    assert back.name == TILE
    np.testing.assert_array_equal(back.count, tiles[0].count)
    assert back.transform == tiles[0].transform


def test_rebuilding_removes_tiles_from_an_earlier_run(bronze, tmp_path):
    (tmp_path / "h99v99.npz").write_bytes(b"stale")
    lai.build_tiles(lai.find_local_scenes([2020], bronze), tmp_path)
    assert [p.name for p in tmp_path.glob("*.npz")] == [f"{TILE}.npz"]


def test_sampling_reads_the_pixel_under_each_point(tiles):
    points = [
        SEATTLE,  # pixel (30, 30)
        _pixel_lonlat(30, 32),  # two composites
        _pixel_lonlat(30, 31),  # urban: no LAI
        (-110.0, 47.0),  # far outside the tile
    ]
    lon, lat = zip(*points, strict=True)
    out = lai.sample_tiles(tiles, lon, lat)
    assert out["lai_summer_mean"][:2].tolist() == pytest.approx([3.0, 4.0])
    assert out["lai_summer_max"][:2].tolist() == pytest.approx([3.0, 5.0])
    assert out["lai_n_obs"].tolist() == [1, 2, 0, 0]
    assert out["modis_tile"].tolist() == [TILE, TILE, TILE, None]
    assert np.isnan(out["lai_summer_mean"][2:]).all()


def test_mosaic_puts_the_value_at_the_right_longitude_and_latitude(tiles):
    bbox = (-122.5, 47.5, -122.2, 47.7)
    grid, transform = lai.mosaic_to_grid(tiles, bbox=bbox, res=0.005)
    col = int((SEATTLE[0] - bbox[0]) / 0.005)
    row = int((bbox[3] - SEATTLE[1]) / 0.005)
    assert grid[row, col] == pytest.approx(3.0, abs=1.0)  # pixels nearby are 3.0, 4.0 or empty
    assert np.isfinite(grid).any()
    assert transform.a == pytest.approx(0.005)


def test_plot_map_writes_a_png(tiles, tmp_path):
    pytest.importorskip("matplotlib")
    grid, _ = lai.mosaic_to_grid(tiles, bbox=(-122.5, 47.5, -122.2, 47.7), res=0.005)
    out = tmp_path / "map.png"
    assert lai.plot_map(grid, None, out, (2020, 2021)) is True
    assert out.stat().st_size > 1000


def test_build_points_writes_one_row_per_occurrence(tiles, tmp_path):
    seattle_gbif = ("1", str(SEATTLE[1]), str(SEATTLE[0]))
    two_comp = _pixel_lonlat(30, 32)
    urban = _pixel_lonlat(30, 31)
    rows = [
        seattle_gbif,
        ("2", str(two_comp[1]), str(two_comp[0])),
        ("3", str(urban[1]), str(urban[0])),
        ("4", "47.0", "-110.0"),  # outside the tile
        ("5", "", "-122.0"),  # no latitude: dropped
    ]
    bronze_file = tmp_path / "occ.parquet"
    con = duckdb.connect()
    con.execute("CREATE TABLE t(gbifID VARCHAR, decimalLatitude VARCHAR, decimalLongitude VARCHAR)")
    con.executemany("INSERT INTO t VALUES (?, ?, ?)", rows)
    con.execute(f"COPY t TO '{bronze_file}' (FORMAT parquet)")
    con.close()

    out = tmp_path / "points.parquet"
    stats = lai.build_points(tiles, bronze_glob=bronze_file, out_path=out)
    assert stats["rows"] == 4 and stats["with_lai"] == 2

    got = duckdb.sql(
        f"SELECT gbif_id, lai_summer_mean, lai_summer_max, lai_n_obs, modis_tile "
        f"FROM read_parquet('{out}') ORDER BY gbif_id"
    ).fetchall()
    assert got[0][0] == 1 and got[0][1] == pytest.approx(3.0) and got[0][3:] == (1, TILE)
    assert got[1][1] == pytest.approx(4.0) and got[1][2] == pytest.approx(5.0)
    assert got[2][1] is None and got[2][3] == 0
    assert got[3][1] is None and got[3][4] is None


def test_build_points_refuses_duplicate_ids(tiles, tmp_path):
    bronze_file = tmp_path / "dup.parquet"
    con = duckdb.connect()
    con.execute("CREATE TABLE t(gbifID VARCHAR, decimalLatitude VARCHAR, decimalLongitude VARCHAR)")
    con.execute("INSERT INTO t VALUES ('1','47.6','-122.3'), ('1','47.7','-122.4')")
    con.execute(f"COPY t TO '{bronze_file}' (FORMAT parquet)")
    con.close()
    with pytest.raises(ValueError, match="duplicate"):
        lai.build_points(tiles, bronze_glob=bronze_file, out_path=tmp_path / "o.parquet")


# ----------------------------------------------------------------------- bronze fetch


class _FakeItem:
    def __init__(self, scene_id, assets):
        self.id = scene_id
        self.assets = {name: SimpleNamespace(href=href) for name, href in assets.items()}
        self.datetime = datetime(2020, 7, 3, tzinfo=UTC)


class _FakeClient:
    def __init__(self, items):
        self._items = items
        self.queries = []

    def search(self, **kwargs):
        self.queries.append(kwargs)
        return SimpleNamespace(items=lambda: iter(self._items))


def _href(name):
    return f"https://example.test/{name}.tif?sig=SECRET-TOKEN"


def test_list_scenes_reads_tile_year_and_urls():
    items = [
        _FakeItem(
            f"MOD15A2H.A2020185.{TILE}.061.a",
            {lai.LAI_ASSET: _href("lai"), lai.QC_ASSET: _href("qc"), "other": "x"},
        )
    ]
    client = _FakeClient(items)
    scenes = lai.list_scenes([2020], client=client)
    assert [(s.tile, s.year) for s in scenes] == [(TILE, 2020)]
    assert set(scenes[0].hrefs) == {lai.LAI_ASSET, lai.QC_ASSET}
    q = client.queries[0]
    assert q["collections"] == [lai.COLLECTION]
    assert q["datetime"] == "2020-07-01/2020-08-31"


def test_list_scenes_names_the_assets_it_did_find_when_one_is_missing():
    items = [_FakeItem(f"MOD15A2H.A2020185.{TILE}.061.a", {"Fpar_500m": _href("f")})]
    with pytest.raises(RuntimeError, match=r"Fpar_500m"):
        lai.list_scenes([2020], client=_FakeClient(items))


class _FakeResponse:
    def __init__(self, payload: bytes, content_length=None):
        self._payload = payload
        self.headers = {
            "Content-Length": str(len(payload) if content_length is None else content_length)
        }

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self._payload), 700):
            yield self._payload[i : i + 700]


def _scene():
    return lai.Scene(
        f"MOD15A2H.A2020185.{TILE}.061.a",
        TILE,
        2020,
        "2020-07-03T00:00:00+00:00",
        {lai.LAI_ASSET: _href("lai"), lai.QC_ASSET: _href("qc")},
    )


def test_fetch_saves_raw_files_manifest_and_skips_what_it_has(bronze, tmp_path, monkeypatch):
    payloads = {
        _href("lai"): next((bronze / "2020").glob(f"*_{lai.LAI_ASSET}.tif")).read_bytes(),
        _href("qc"): next((bronze / "2020").glob(f"*_{lai.QC_ASSET}.tif")).read_bytes(),
    }
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return _FakeResponse(payloads[url])

    monkeypatch.setattr(lai.requests, "get", fake_get)
    stats = lai.fetch_scenes([_scene()], bronze_dir=tmp_path, sign=lambda h: h)

    assert stats["new_files"] == 2 and stats["skipped_files"] == 0
    saved = sorted(p.name for p in (tmp_path / "2020").iterdir())
    assert saved == sorted(
        [
            f"MOD15A2H.A2020185.{TILE}.061.a_{lai.LAI_ASSET}.tif",
            f"MOD15A2H.A2020185.{TILE}.061.a_{lai.QC_ASSET}.tif",
        ]
    )
    for path in (tmp_path / "2020").iterdir():  # byte for byte what the server sent
        assert path.read_bytes() in payloads.values()

    manifest = (tmp_path / "manifest.csv").read_text()
    assert manifest.splitlines()[0].split(",") == lai.MANIFEST_COLUMNS
    assert len(manifest.splitlines()) == 3
    assert "SECRET-TOKEN" not in manifest  # signed tokens are never stored

    again = lai.fetch_scenes([_scene()], bronze_dir=tmp_path, sign=lambda h: h)
    assert again["new_files"] == 0 and again["skipped_files"] == 2
    assert len(calls) == 2


def test_fetch_retries_then_gives_up_without_leaving_partial_files(tmp_path, monkeypatch):
    monkeypatch.setattr(lai.time, "sleep", lambda s: None)
    attempts = []

    def failing_get(url, **kwargs):
        attempts.append(url)
        raise lai.requests.ConnectionError("network down")

    monkeypatch.setattr(lai.requests, "get", failing_get)
    with pytest.raises(RuntimeError, match="Could not download"):
        lai.fetch_scenes([_scene()], bronze_dir=tmp_path, sign=lambda h: h)
    assert len(attempts) == 3
    assert not list(tmp_path.rglob("*.part")) and not list(tmp_path.rglob("*.tif"))


def test_fetch_rejects_a_file_that_is_not_a_raster(tmp_path, monkeypatch):
    pytest.importorskip("rasterio")
    monkeypatch.setattr(lai.requests, "get", lambda url, **kw: _FakeResponse(b"<html>nope</html>"))
    with pytest.raises(ValueError, match="not a readable raster"):
        lai.fetch_scenes([_scene()], bronze_dir=tmp_path, sign=lambda h: h)
    assert not list(tmp_path.rglob("*.tif"))
