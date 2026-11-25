"""Cross-validation folds. Spatial blocks keep neighbours together, so test folds are new ground."""

import numpy as np


def assign_random_folds(n: int, k: int, seed: int = 0) -> np.ndarray:
    """Plain random folds. Reported next to spatial CV to show how much random CV inflates."""
    rng = np.random.default_rng(seed)
    folds = np.arange(n) % k
    rng.shuffle(folds)
    return folds


def assign_spatial_folds(lon, lat, k: int = 5, block_deg: float = 1.0, seed: int = 0) -> np.ndarray:
    """Assign each point to one of k folds by the square block (block_deg degrees) it falls in.

    Whole blocks go to one fold, so no test point has a training point in its own block. Blocks are
    handed out greedily, largest first, to the fold with the fewest points so far, which keeps
    fold sizes close even when records cluster around cities.
    """
    lon, lat = np.asarray(lon, dtype=float), np.asarray(lat, dtype=float)
    bx = np.floor(lon / block_deg).astype(int)
    by = np.floor(lat / block_deg).astype(int)
    _, block_id = np.unique(np.stack([bx, by], axis=1), axis=0, return_inverse=True)
    block_id = block_id.ravel()
    counts = np.bincount(block_id)
    if len(counts) < k:
        raise ValueError(
            f"Only {len(counts)} spatial blocks for k={k} folds; use a smaller --block-deg or --k."
        )
    rng = np.random.default_rng(seed)
    order = np.lexsort((rng.random(len(counts)), -counts))  # biggest first, ties shuffled
    load = np.zeros(k, dtype=int)
    block_fold = np.empty(len(counts), dtype=int)
    for b in order:
        f = int(load.argmin())
        block_fold[b] = f
        load[f] += counts[b]
    return block_fold[block_id]
