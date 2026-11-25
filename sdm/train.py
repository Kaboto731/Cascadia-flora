"""Train and evaluate species distribution models on present-day climate.

    python -m sdm.train --res 30s --background target_group

For each species it compares spatial-block CV with random CV (the gap is how much random CV
flatters a model), writes cv_metrics.csv, saves a final model, and draws a suitability map.
Presence-only data gives RELATIVE suitability, not the probability a species occurs.
"""

import argparse
import json
import re
import sys
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from pipelines.ingest_gbif import REPO_ROOT
from pipelines.ingest_worldclim import sample_stack, stack_file
from sdm import metrics
from sdm.background import sample_background, valid_pixels
from sdm.cv import assign_random_folds, assign_spatial_folds
from sdm.data import load_presences
from sdm.features import BIO_COLS, prune_correlated
from sdm.model import MODEL_NAMES, make_model

DEFAULT_DB = REPO_ROOT / "data" / "cascadia.duckdb"
DEFAULT_OUT = REPO_ROOT / "data" / "gold" / "sdm"
MIN_TEST_PRESENCES = 5


def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def build_dataset(pres, other, stack_path, n_background, mode, seed, corr_threshold):
    """Presence + background table with climate columns, plus the pruned feature list."""
    blon, blat = sample_background(
        stack_path,
        n_background,
        seed=seed,
        mode=mode,
        target_lonlat=other[["lon", "lat"]].to_numpy() if len(other) else None,
    )
    lon = np.concatenate([pres["lon"].to_numpy(), blon])
    lat = np.concatenate([pres["lat"].to_numpy(), blat])
    y = np.concatenate([np.ones(len(pres), int), np.zeros(len(blon), int)])
    vals, _ = sample_stack(stack_path, lon, lat)
    df = pd.DataFrame(vals, columns=BIO_COLS)
    df["lon"], df["lat"], df["y"] = lon, lat, y
    df = df.dropna(subset=BIO_COLS).reset_index(drop=True)
    return df, prune_correlated(df, BIO_COLS, corr_threshold)


def cross_validate(df, features, models, scheme, k, block_deg, seed):
    if scheme == "spatial":
        folds = assign_spatial_folds(df["lon"], df["lat"], k, block_deg, seed)
    else:
        folds = assign_random_folds(len(df), k, seed)
    X, y = df[features].to_numpy(), df["y"].to_numpy()
    rows = []
    for name in models:
        for f in range(k):
            tr, te = folds != f, folds == f
            if y[te].sum() < MIN_TEST_PRESENCES or y[tr].sum() == 0 or (y[tr] == 0).sum() == 0:
                continue
            m = make_model(name, seed, pos_weight=(y[tr] == 0).sum() / y[tr].sum())
            m.fit(X[tr], y[tr])
            p_tr, p_te = m.predict_proba(X[tr])[:, 1], m.predict_proba(X[te])[:, 1]
            thr = metrics.best_threshold(y[tr], p_tr)  # threshold never sees the test fold
            rows.append(
                {
                    "model": name,
                    "cv": scheme,
                    "fold": f,
                    "n_train": int(tr.sum()),
                    "n_test_presences": int(y[te].sum()),
                    "auc": metrics.auc(y[te], p_te),
                    "tss": metrics.tss(y[te], p_te, thr),
                    "boyce": metrics.boyce(p_te[y[te] == 1], p_te[y[te] == 0]),
                }
            )
    return rows


def predict_map(model, features, stack_path):
    """Suitability grid over the study area (NaN outside it) and its transform."""
    import rasterio

    with rasterio.open(stack_path) as src:
        stack, transform = src.read(), src.transform
    rows, cols, _ = valid_pixels(stack_path)
    idx = [int(f.split("_")[1]) - 1 for f in features]
    X = stack[idx][:, rows, cols].T
    ok = ~np.isnan(X).any(axis=1)
    grid = np.full(stack.shape[1:], np.nan, dtype="float32")
    grid[rows[ok], cols[ok]] = model.predict_proba(X[ok])[:, 1]
    return grid, transform


def write_geotiff(grid, transform, path: Path, description: str) -> None:
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
        dst.write(grid, 1)
        dst.set_band_description(1, description)


def plot_map(grid, transform, pres, title, path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap

    cmap = LinearSegmentedColormap.from_list("blues", ["#cde2fb", "#6fa8e8", "#1f5fae", "#0d366b"])
    h, w = grid.shape
    extent = (
        transform.c,
        transform.c + w * transform.a,
        transform.f + h * transform.e,
        transform.f,
    )
    fig, ax = plt.subplots(figsize=(7, 7 * h / w * 1.1), dpi=150)
    im = ax.imshow(grid, extent=extent, cmap=cmap, vmin=0, vmax=1, interpolation="nearest")
    ax.scatter(pres["lon"], pres["lat"], s=3, c="#d95f02", alpha=0.6, linewidths=0)
    ax.set_title(title, loc="left", fontsize=11)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    fig.colorbar(im, ax=ax, shrink=0.7, label="Relative suitability (not a probability)")
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path)
    plt.close(fig)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--db", type=Path, default=DEFAULT_DB)
    ap.add_argument("--res", default="30s", help="WorldClim resolution of the climate stack")
    ap.add_argument("--stack", type=Path, default=None, help="Override the climate GeoTIFF path")
    ap.add_argument("--species", nargs="*", default=None, help="Names (substring) or species keys")
    ap.add_argument("--background", choices=["target_group", "random"], default="target_group")
    ap.add_argument("--n-background", type=int, default=10000)
    ap.add_argument("--block-deg", type=float, default=1.0)
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--models", nargs="+", default=list(MODEL_NAMES), choices=MODEL_NAMES)
    ap.add_argument("--map-model", default="rf", choices=MODEL_NAMES)
    ap.add_argument("--corr-threshold", type=float, default=0.7)
    ap.add_argument("--min-presences", type=int, default=30)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--no-maps", action="store_true")
    args = ap.parse_args(argv)

    stack_path = args.stack or stack_file(args.res)
    if not Path(stack_path).exists():
        sys.exit(
            f"Climate stack not found: {stack_path}. "
            f"Run: python -m pipelines.ingest_worldclim clip --res {args.res}"
        )
    allp = load_presences(args.db)
    mismatch = allp["worldclim_res"].dropna().unique()
    if len(mismatch) and set(mismatch) != {args.res}:
        warnings.warn(
            f"Presences were thinned/sampled at {list(mismatch)} but --res is {args.res}; "
            "rebuild dbt with a matching thin_cells_per_degree.",
            stacklevel=1,
        )

    species = allp[["species_key", "species_name"]].drop_duplicates()
    if args.species:
        keep = [str(s).lower() for s in args.species]
        species = species[
            species.apply(
                lambda r: (
                    str(r.species_key) in keep or any(s in r.species_name.lower() for s in keep)
                ),
                axis=1,
            )
        ]

    args.out_dir.mkdir(parents=True, exist_ok=True)
    all_rows, summary = [], {}
    for key, name in species.itertuples(index=False):
        pres = allp[allp["species_key"] == key]
        other = allp[allp["species_key"] != key]
        if len(pres) < args.min_presences:
            print(f"skip {name}: {len(pres)} thinned presences (< {args.min_presences})")
            continue
        print(f"{name}: {len(pres)} presences")
        df, feats = build_dataset(
            pres,
            other,
            stack_path,
            args.n_background,
            args.background,
            args.seed,
            args.corr_threshold,
        )
        rows = []
        for scheme in ("spatial", "random"):
            rows += cross_validate(
                df, feats, args.models, scheme, args.k, args.block_deg, args.seed
            )
        for r in rows:
            r.update(species=name, background=args.background)
        all_rows += rows

        X, y = df[feats].to_numpy(), df["y"].to_numpy()
        final = make_model(args.map_model, args.seed, pos_weight=(y == 0).sum() / y.sum()).fit(X, y)
        sdir = args.out_dir / slug(name)
        sdir.mkdir(parents=True, exist_ok=True)
        joblib.dump(
            {"model": final, "features": feats, "species": name}, sdir / f"{args.map_model}.joblib"
        )
        if not args.no_maps:
            grid, transform = predict_map(final, feats, stack_path)
            write_geotiff(grid, transform, sdir / "suitability.tif", f"{name} relative suitability")
            plot_map(
                grid,
                transform,
                pres,
                f"{name}: relative climate suitability ({args.map_model})",
                sdir / "suitability.png",
            )
        summary[name] = {
            "n_presences": int(y.sum()),
            "n_background": int((y == 0).sum()),
            "features": feats,
            "background": args.background,
        }

    if not all_rows:
        sys.exit("No species had enough presences to model.")
    out = pd.DataFrame(all_rows)
    out.to_csv(args.out_dir / "cv_metrics.csv", index=False)
    (args.out_dir / "run_summary.json").write_text(json.dumps(summary, indent=2))
    table = out.groupby(["species", "model", "cv"])[["auc", "tss", "boyce"]].mean().round(3)
    print(table.to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
