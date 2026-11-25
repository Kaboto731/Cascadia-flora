"""Evaluation metrics for presence/background models."""

import numpy as np
from scipy.stats import spearmanr
from sklearn.metrics import roc_auc_score


def auc(y, p) -> float:
    y = np.asarray(y)
    if y.min() == y.max():
        return float("nan")
    return float(roc_auc_score(y, p))


def best_threshold(y, p) -> float:
    """Threshold that maximizes TSS (sensitivity + specificity - 1). Fit this on TRAINING data."""
    y, p = np.asarray(y).astype(bool), np.asarray(p, dtype=float)
    cands = np.unique(p)
    if len(cands) > 200:
        cands = np.quantile(p, np.linspace(0, 1, 201))
    best, best_t = -2.0, 0.5
    for t in cands:
        pred = p >= t
        sens = (pred & y).sum() / max(y.sum(), 1)
        spec = (~pred & ~y).sum() / max((~y).sum(), 1)
        if sens + spec - 1 > best:
            best, best_t = sens + spec - 1, float(t)
    return best_t


def tss(y, p, threshold: float) -> float:
    y, pred = np.asarray(y).astype(bool), np.asarray(p, dtype=float) >= threshold
    sens = (pred & y).sum() / max(y.sum(), 1)
    spec = (~pred & ~y).sum() / max((~y).sum(), 1)
    return float(sens + spec - 1)


def boyce(p_presence, p_background, n_windows: int = 100, width: float = 0.1) -> float:
    """Continuous Boyce index: do held-out presences land in high-suitability areas more often
    than the available background would predict? Ranges -1..1; near 0 is no better than random.
    Works with presence-only data, since it never needs true absences."""
    pp, pb = np.asarray(p_presence, dtype=float), np.asarray(p_background, dtype=float)
    lo, hi = float(pb.min()), float(pb.max())
    if hi <= lo or len(pp) < 5:
        return float("nan")
    w = width * (hi - lo)
    centers, ratios = [], []
    for c in np.linspace(lo + w / 2, hi - w / 2, n_windows):
        a, b = c - w / 2, c + w / 2
        e = ((pb >= a) & (pb <= b)).mean()
        if e == 0:
            continue
        centers.append(c)
        ratios.append(((pp >= a) & (pp <= b)).mean() / e)
    if len(centers) < 3 or np.allclose(ratios, ratios[0]):
        return float("nan")
    return float(spearmanr(centers, ratios).statistic)
