"""Predictor selection. The 19 bioclim variables are highly correlated; prune them first."""

import pandas as pd

BIO_COLS = [f"bio_{i}" for i in range(1, 20)]

# Kept first when two predictors are correlated: broad, interpretable climate axes.
PREFERRED = [
    "bio_1",  # annual mean temperature
    "bio_12",  # annual precipitation
    "bio_17",  # precipitation of driest quarter (summer drought)
    "bio_4",  # temperature seasonality
    "bio_15",  # precipitation seasonality
    "bio_5",  # max temperature of warmest month
    "bio_6",  # min temperature of coldest month
    "bio_14",  # precipitation of driest month --top 8 ends here confouding
    "bio_2", # Mean diurnal range: average of the monthly (max temp − min temp)
    "bio_3", # Isothermality: bio_2 divided by bio_7, times 100 (how much day-night swing matters compared with the summer-winter swing)
    "bio_7", # Temperature annual range: max temp of the warmest month minus min temp of the coldest month
    "bio_8", # Mean temperature of the wettest quarter
    "bio_9", # Mean temperature of the driest quarter
    "bio_10", # Mean temperature of the warmest quarter
    "bio_11", # Mean temperature of the coldest quarter
    "bio_13", # Precipitation of the wettest month
    "bio_16", # Precipitation of the wettest quarter
    "bio_18", # Precipitation of the warmest quarter
    "bio_19", # Precipitation of the coldest quarter
]


def prune_correlated(
    df: pd.DataFrame, cols: list[str] | None = None, threshold: float = 0.7
) -> list[str]:
    """Greedy pruning: walk PREFERRED order; drop a variable if |r| with a kept one > threshold."""
    cols = [c for c in (cols or BIO_COLS) if c in df.columns]
    ordered = [c for c in PREFERRED if c in cols] + [c for c in cols if c not in PREFERRED]
    corr = df[ordered].corr().abs()
    kept: list[str] = []
    for c in ordered:
        if all(corr.loc[c, k] <= threshold for k in kept):
            kept.append(c)
    return kept
