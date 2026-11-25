"""Tests for the sdm package, on synthetic climate with a species whose niche is known."""

import duckdb
import numpy as np
import pandas as pd
import pytest

rasterio = pytest.importorskip("rasterio")
pytest.importorskip("sklearn")

from rasterio.transform import from_origin  # noqa: E402

from sdm import background, train  # noqa: E402
from sdm.cv import assign_random_folds, assign_spatial_folds  # noqa: E402
from sdm.features import prune_correlated  # noqa: E402
from sdm.metrics import auc, best_threshold, boyce, tss  # noqa: E402
from sdm.model import make_model  # noqa: E402

H, W, RES = 80, 120, 0.1  # 12 x 8 degrees, origin at lon -124, lat 50
LON0, LAT0 = -124.0, 50.0


def _pixel_lonlat(rows, cols):
    return LON0 + (cols + 0.5) * RES, LAT0 - (rows + 0.5) * RES


@pytest.fixture(scope="module")
def stack_path(tmp_path_factory):
    """19 bands: bio_1 falls with latitude, bio_12 rises with longitude; the rest is noise."""
    rng = np.random.default_rng(1)
    rr, cc = np.mgrid[0:H, 0:W]
    bands = [np.zeros((H, W), "float32") for _ in range(19)]
    bands[0] = 14 - rr * 0.1 + rng.normal(0, 0.05, (H, W))  # bio_1: 14 down to 6
    bands[11] = 500 + cc * 12 + rng.normal(0, 5, (H, W))  # bio_12
    bands[3] = bands[0] * 3 + rng.normal(0, 0.1, (H, W))  # bio_4, nearly collinear with bio_1
    for i in range(19):
        if i not in (0, 3, 11):
            bands[i] = rng.normal(i, 1, (H, W))
    arr = np.stack(bands).astype("float32")
    arr[:, :3, :] = np.nan  # a no-data strip
    path = tmp_path_factory.mktemp("sdm") / "bio_test_cascadia.tif"
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=H,
        width=W,
        count=19,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(LON0, LAT0, RES, RES),
        nodata=np.nan,
    ) as dst:
        dst.write(arr)
    return path


@pytest.fixture(autouse=True)
def _no_polygon(monkeypatch):
    monkeypatch.setattr(background, "study_area_mask", lambda shape, transform: None)


def _species_points(n, seed, lo, hi):
    """Presences scattered where bio_1 is within [lo, hi]; bio_1 = 14 - row*0.1."""
    rng = np.random.default_rng(seed)
    r_lo, r_hi = int((14 - hi) / 0.1), int((14 - lo) / 0.1)
    rows = rng.integers(max(r_lo, 4), r_hi, n)
    cols = rng.integers(0, W, n)
    lon, lat = _pixel_lonlat(rows, cols)
    return lon, lat


def test_spatial_folds_keep_blocks_together_and_balanced():
    rng = np.random.default_rng(0)
    lon, lat = rng.uniform(-124, -112, 2000), rng.uniform(42, 50, 2000)
    folds = assign_spatial_folds(lon, lat, k=5, block_deg=1.0, seed=3)
    blocks = pd.Series(list(zip(np.floor(lon), np.floor(lat), strict=True)))
    assert (pd.DataFrame({"b": blocks, "f": folds}).groupby("b")["f"].nunique() == 1).all()
    sizes = np.bincount(folds)
    assert sizes.min() > 0.7 * sizes.max()


def test_spatial_folds_too_few_blocks_raises():
    with pytest.raises(ValueError, match="spatial blocks"):
        assign_spatial_folds([0.1, 0.2], [0.1, 0.2], k=5, block_deg=10)


def test_random_folds_cover_all():
    assert set(assign_random_folds(100, 5)) == {0, 1, 2, 3, 4}


def test_prune_correlated_keeps_preferred_and_drops_collinear():
    rng = np.random.default_rng(0)
    a = rng.normal(size=500)
    df = pd.DataFrame(
        {"bio_1": a, "bio_4": a * 2 + rng.normal(0, 0.01, 500), "bio_12": rng.normal(size=500)}
    )
    kept = prune_correlated(df, ["bio_1", "bio_4", "bio_12"], 0.7)
    assert kept == ["bio_1", "bio_12"]


def test_metrics_perfect_and_random():
    y = np.array([1] * 50 + [0] * 50)
    perfect = np.where(y == 1, 0.9, 0.1)
    assert auc(y, perfect) == 1.0
    assert tss(y, perfect, best_threshold(y, perfect)) == 1.0
    rng = np.random.default_rng(0)
    noise = rng.random(2000)
    yy = (rng.random(2000) < 0.3).astype(int)
    assert abs(auc(yy, noise) - 0.5) < 0.06


def test_boyce_positive_when_presences_in_high_suitability():
    rng = np.random.default_rng(0)
    bg = rng.random(3000)
    good = np.clip(rng.normal(0.8, 0.1, 200), 0, 1)
    assert boyce(good, bg) > 0.7
    assert abs(boyce(rng.random(200), bg)) < 0.5


def test_random_background_is_on_valid_pixels(stack_path):
    lon, lat = background.sample_background(stack_path, 500, seed=2, mode="random")
    assert len(lon) == 500
    assert (lat < LAT0 - 3 * RES).all()  # never the no-data strip
    assert lon.min() >= LON0 and lon.max() <= LON0 + W * RES


def test_target_group_background_draws_from_pool(stack_path):
    pool = np.array([[-120.0, 45.0], [-121.0, 44.0], [-122.0, 46.0]])
    lon, lat = background.sample_background(stack_path, 10, mode="target_group", target_lonlat=pool)
    assert len(lon) == 3 and {tuple(p) for p in zip(lon, lat, strict=True)} == {
        tuple(p) for p in pool
    }
    with pytest.raises(ValueError):
        background.sample_background(stack_path, 10, mode="target_group")


@pytest.mark.parametrize("name", ["logistic", "rf", "xgboost"])
def test_models_learn_the_known_niche(name, stack_path):
    if name == "xgboost":
        pytest.importorskip("xgboost")
    lon, lat = _species_points(300, 0, 9.0, 10.5)
    pres = pd.DataFrame({"lon": lon, "lat": lat})
    other = pd.DataFrame(
        {
            "lon": np.random.default_rng(5).uniform(-123, -113, 800),
            "lat": np.random.default_rng(6).uniform(42.5, 49.5, 800),
        }
    )
    df, feats = train.build_dataset(pres, other, stack_path, 1500, "random", 0, 0.7)
    assert "bio_4" not in feats and "bio_1" in feats  # collinear partner dropped
    rows = train.cross_validate(df, feats, [name], "spatial", 4, 1.0, 0)
    assert len(rows) >= 3
    # a perfect model scores AUC 0.90 here (the niche is a band, background is everywhere)
    assert np.mean([r["auc"] for r in rows]) > 0.75
    assert np.mean([r["boyce"] for r in rows]) > 0.15  # Boyce is noisy fold to fold


def test_model_factory_rejects_unknown():
    with pytest.raises(ValueError):
        make_model("nope")


def test_train_main_end_to_end(tmp_path, stack_path):
    frames = []
    for key, name, (lo, hi), seed in [
        (1, "Niche warm", (10.0, 12.0), 1),
        (2, "Niche cool", (7.0, 8.5), 2),
        (3, "Too rare", (9.0, 10.0), 3),
    ]:
        n = 10 if key == 3 else 250
        lon, lat = _species_points(n, seed, lo, hi)
        frames.append(
            pd.DataFrame(
                {
                    "species_key": key,
                    "species_name": name,
                    "lon": lon,
                    "lat": lat,
                    "worldclim_res": "test",
                }
            )
        )
    pres = pd.concat(frames, ignore_index=True)
    db = tmp_path / "t.duckdb"
    con = duckdb.connect(str(db))
    con.register("p", pres)
    con.execute("create table int_occurrences__thinned as select * from p")
    con.close()

    out = tmp_path / "out"
    code = train.main(
        [
            "--db",
            str(db),
            "--stack",
            str(stack_path),
            "--res",
            "test",
            "--out-dir",
            str(out),
            "--models",
            "logistic",
            "rf",
            "--n-background",
            "1000",
            "--k",
            "4",
            "--map-model",
            "rf",
            "--background",
            "random",
        ]
    )
    assert code == 0
    m = pd.read_csv(out / "cv_metrics.csv")
    assert set(m["species"]) == {"Niche warm", "Niche cool"}  # rare species skipped
    assert set(m["cv"]) == {"spatial", "random"}
    assert m["auc"].between(0, 1).all()
    assert (out / "niche_warm" / "suitability.tif").exists()
    assert (out / "niche_warm" / "suitability.png").exists()
    with rasterio.open(out / "niche_warm" / "suitability.tif") as src:
        grid = src.read(1)
    # warm species: suitability higher in the warm (south, large row index? no: low row) rows
    warm_rows = grid[10:30]  # bio_1 12.9..11.1 range (warm)
    cool_rows = grid[60:75]  # bio_1 ~8..6.5 (cool)
    assert np.nanmean(warm_rows) > np.nanmean(cool_rows) + 0.2
    assert np.isnan(grid[:3]).all()
