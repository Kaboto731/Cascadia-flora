"""Offline tests for the WorldClim pipeline, using a tiny synthetic raster zip.

The synthetic grid is 0.5 degree cells over -125..-120 W, 45..49 N (10 columns x 8 rows).
Band n, row r, column c holds n * 1000 + r * 10 + c, so every sampled value says exactly
which band and pixel it came from. Column 0 of every band is no-data ("ocean").
"""

import zipfile
from pathlib import Path

import duckdb
import numpy as np
import pytest

from pipelines import ingest_worldclim as wc

RES = "10m"
BBOX = (-125.0, 45.0, -120.0, 49.0)
NODATA = -3.4028235e38
ROWS, COLS, CELL = 8, 10, 0.5


def _pixel_value(n: int, r: int, c: int) -> float:
    return n * 1000 + r * 10 + c


def _lon(c: int) -> float:
    """Longitude of the middle of column c."""
    return -125.0 + (c + 0.5) * CELL


def _lat(r: int) -> float:
    """Latitude of the middle of row r."""
    return 49.0 - (r + 0.5) * CELL


@pytest.fixture(scope="module")
def zip_path(tmp_path_factory) -> Path:
    rasterio = pytest.importorskip("rasterio")
    from rasterio.transform import from_origin

    folder = tmp_path_factory.mktemp("wc")
    transform = from_origin(-125.0, 49.0, CELL, CELL)
    out = folder / wc.zip_name(RES)
    with zipfile.ZipFile(out, "w") as zf:
        for n in range(1, wc.N_BIO + 1):
            data = np.array(
                [[_pixel_value(n, r, c) for c in range(COLS)] for r in range(ROWS)],
                dtype="float32",
            )
            data[:, 0] = NODATA
            tif = folder / wc.member_name(RES, n)
            with rasterio.open(
                tif,
                "w",
                driver="GTiff",
                dtype="float32",
                count=1,
                height=ROWS,
                width=COLS,
                crs="EPSG:4326",
                transform=transform,
                nodata=NODATA,
            ) as dst:
                dst.write(data, 1)
            zf.write(tif, arcname=tif.name)
    return out


@pytest.fixture(scope="module")
def stack(zip_path, tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("silver") / "stack.tif"
    return wc.clip_to_bbox(zip_path, RES, out, bbox=BBOX)


# ----------------------------------------------------------------------- pure helpers


def test_names_and_url():
    assert wc.zip_name("30s") == "wc2.1_30s_bio.zip"
    assert wc.member_name("30s", 12) == "wc2.1_30s_bio_12.tif"
    assert wc.zip_url("10m") == (
        "https://geodata.ucdavis.edu/climate/worldclim/2_1/base/wc2.1_10m_bio.zip"
    )


def test_every_bio_variable_is_described():
    assert sorted(wc.BIO_NAMES) == list(range(1, wc.N_BIO + 1))


def test_check_zip_rejects_a_zip_with_missing_bands(tmp_path):
    bad = tmp_path / "bad.zip"
    with zipfile.ZipFile(bad, "w") as zf:
        zf.writestr(wc.member_name(RES, 1), b"x")
    with pytest.raises(ValueError, match="missing bands"):
        wc.check_zip(bad, RES)


def test_check_zip_rejects_a_truncated_file(tmp_path):
    bad = tmp_path / "cut.zip"
    bad.write_bytes(b"PK\x03\x04 not really a zip")
    with pytest.raises(ValueError, match="not a valid zip"):
        wc.check_zip(bad, RES)


# ----------------------------------------------------------------------- download


class _FakeResponse:
    def __init__(self, payload: bytes):
        self._payload = payload
        self.headers = {"Content-Length": str(len(payload))}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self._payload), 1000):
            yield self._payload[i : i + 1000]


def test_download_saves_file_and_writes_manifest(zip_path, tmp_path, monkeypatch):
    payload = zip_path.read_bytes()
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return _FakeResponse(payload)

    monkeypatch.setattr(wc.requests, "get", fake_get)
    dest = wc.download(RES, dest_dir=tmp_path)

    assert dest.read_bytes() == payload
    assert not list(tmp_path.glob("*.part"))
    rows = (tmp_path / "manifest.csv").read_text().splitlines()
    assert rows[0].split(",") == wc.MANIFEST_COLUMNS
    assert len(rows) == 2
    assert wc.zip_url(RES) in rows[1]

    wc.download(RES, dest_dir=tmp_path)  # second call must not fetch again
    assert len(calls) == 1


def test_download_rejects_a_cut_short_transfer(tmp_path, monkeypatch):
    class Short(_FakeResponse):
        def __init__(self):
            super().__init__(b"x" * 100)
            self.headers = {"Content-Length": "5000"}

    monkeypatch.setattr(wc.requests, "get", lambda url, **kw: Short())
    with pytest.raises(OSError, match="cut short"):
        wc.download(RES, dest_dir=tmp_path)
    assert not list(tmp_path.glob("*.zip*"))


# ----------------------------------------------------------------------- clip


def test_clip_makes_a_nineteen_band_stack(stack):
    rasterio = pytest.importorskip("rasterio")
    with rasterio.open(stack) as src:
        assert src.count == wc.N_BIO
        assert (src.height, src.width) == (ROWS, COLS)
        assert src.descriptions[0] == "bio_1"
        assert src.descriptions[-1] == "bio_19"
        data = src.read()
    assert np.isnan(data[:, :, 0]).all()  # no-data column became NaN
    assert data[4, 3, 5] == _pixel_value(5, 3, 5)  # band 5, row 3, column 5


def test_clip_to_a_smaller_box_keeps_only_that_window(zip_path, tmp_path):
    rasterio = pytest.importorskip("rasterio")
    out = wc.clip_to_bbox(zip_path, RES, tmp_path / "small.tif", bbox=(-124.0, 46.0, -122.0, 48.0))
    with rasterio.open(out) as src:
        assert src.width < COLS
        assert src.height < ROWS
        first = src.read(1)
    assert not np.isnan(first).any()


# ----------------------------------------------------------------------- sampling


def test_sample_returns_the_pixel_values(stack):
    values, snapped = wc.sample_stack(stack, [_lon(4)], [_lat(2)], snap=0)
    expected = [_pixel_value(n, 2, 4) for n in range(1, wc.N_BIO + 1)]
    assert values[0].tolist() == expected
    assert not snapped[0]


def test_point_outside_the_raster_is_nan(stack):
    values, snapped = wc.sample_stack(stack, [-130.0], [47.0], snap=1)
    assert np.isnan(values).all()
    assert not snapped[0]


def test_no_data_pixel_stays_empty_without_snap(stack):
    values, snapped = wc.sample_stack(stack, [_lon(0)], [_lat(3)], snap=0)
    assert np.isnan(values).all()
    assert not snapped[0]


def test_snap_borrows_the_nearest_land_pixel(stack):
    values, snapped = wc.sample_stack(stack, [_lon(0)], [_lat(3)], snap=1)
    assert snapped[0]
    assert values[0, 0] == _pixel_value(1, 3, 1)  # the land pixel next door


def test_snap_does_not_reach_beyond_its_distance(stack):
    # Column 1 is land; a point in column 0 snaps at distance 1, but the raster edge
    # leaves nothing within reach for a point whose whole neighbourhood is no-data.
    values, snapped = wc.sample_stack(stack, [_lon(0)], [_lat(3)], snap=0)
    assert np.isnan(values).all() and not snapped.any()


# ----------------------------------------------------------------------- end to end


def test_build_points_writes_one_row_per_occurrence(stack, tmp_path):
    bronze = tmp_path / "occ.parquet"
    con = duckdb.connect()
    con.execute("CREATE TABLE t(gbifID VARCHAR, decimalLatitude VARCHAR, decimalLongitude VARCHAR)")
    con.executemany(
        "INSERT INTO t VALUES (?, ?, ?)",
        [
            ("1", str(_lat(2)), str(_lon(4))),  # land
            ("2", str(_lat(3)), str(_lon(0))),  # no-data column: snaps
            ("3", "47.0", "-130.0"),  # outside the raster
            ("4", "", "-122.0"),  # no latitude: dropped
            ("5", "abc", "-122.0"),  # unparseable: dropped
        ],
    )
    con.execute(f"COPY t TO '{bronze}' (FORMAT parquet)")
    con.close()

    out = tmp_path / "points.parquet"
    stats = wc.build_points(RES, bronze_glob=bronze, stack_path=stack, out_path=out, snap=1)

    assert stats["points"] == 3
    assert stats["with_climate"] == 2
    assert stats["snapped"] == 1
    assert stats["no_climate"] == 1

    rows = duckdb.sql(
        f"SELECT gbif_id, bio_1, bio_19, climate_snapped, worldclim_res "
        f"FROM read_parquet('{out}') ORDER BY gbif_id"
    ).fetchall()
    assert rows[0] == (1, _pixel_value(1, 2, 4), _pixel_value(19, 2, 4), False, RES)
    assert rows[1][0] == 2 and rows[1][3] is True
    assert rows[2][0] == 3 and rows[2][1] is None


def test_build_points_refuses_duplicate_ids(stack, tmp_path):
    bronze = tmp_path / "dup.parquet"
    con = duckdb.connect()
    con.execute("CREATE TABLE t(gbifID VARCHAR, decimalLatitude VARCHAR, decimalLongitude VARCHAR)")
    con.execute("INSERT INTO t VALUES ('1','47.0','-122.0'), ('1','47.1','-122.1')")
    con.execute(f"COPY t TO '{bronze}' (FORMAT parquet)")
    con.close()
    with pytest.raises(ValueError, match="duplicate"):
        wc.build_points(RES, bronze_glob=bronze, stack_path=stack, out_path=tmp_path / "o.parquet")
