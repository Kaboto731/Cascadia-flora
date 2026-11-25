"""Background (pseudo-absence) sampling. Presence-only data has no true absences, so the model
contrasts presences against points that describe the climate available in the study area."""

import numpy as np

from pipelines.ingest_lai import study_area_mask


def valid_pixels(stack_path):
    """Return (rows, cols, transform): land pixels with climate, inside the study polygon."""
    import rasterio

    with rasterio.open(stack_path) as src:
        valid = ~np.isnan(src.read(1))
        transform = src.transform
    mask = study_area_mask(valid.shape, transform)
    if mask is not None:
        valid &= mask
    rows, cols = np.nonzero(valid)
    return rows, cols, transform


def sample_background(stack_path, n, seed=0, mode="random", target_lonlat=None):
    """Return (lon, lat) arrays of background points.

    random        uniform over valid pixels in the study area (jittered inside the pixel)
    target_group  drawn from records of other species (shared collector bias: if observers
                  favour roads, the background favours roads too, so the model cannot just
                  learn "near roads"). target_lonlat is an (m, 2) array of lon, lat.
    """
    rng = np.random.default_rng(seed)
    if mode == "target_group":
        if target_lonlat is None or len(target_lonlat) == 0:
            raise ValueError("target_group background needs records of other species")
        pool = np.asarray(target_lonlat, dtype=float)
        idx = rng.choice(len(pool), size=min(n, len(pool)), replace=False)
        return pool[idx, 0], pool[idx, 1]
    if mode != "random":
        raise ValueError(f"Unknown background mode: {mode}")
    rows, cols, t = valid_pixels(stack_path)
    if len(rows) == 0:
        raise ValueError("No valid pixels to sample background from")
    pick = rng.integers(0, len(rows), size=n)
    r = rows[pick] + rng.random(n)
    c = cols[pick] + rng.random(n)
    lon = t.a * c + t.b * r + t.c
    lat = t.d * c + t.e * r + t.f
    return lon, lat
