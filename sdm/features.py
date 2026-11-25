"""Predictor selection. The 19 bioclim variables are highly correlated; prune them first."""

import pandas as pd

BIO_COLS = [f"bio_{i}" for i in range(1, 20)]

# Order of preference: when two predictors are correlated, the earlier one is kept.
# The first eight are broad, interpretable climate axes. The rest follow, and are usually
# dropped because they overlap with the earlier ones.
#
# WorldClim notes: temperatures are in degrees C and precipitation in mm. A "quarter" is any
# three consecutive months. bio_3 (isothermality) is bio_2 divided by bio_7, times 100: how
# large the day-night swing is compared with the summer-winter swing.
PREFERRED = [
    "bio_1",  # annual mean temperature
    "bio_12",  # annual precipitation
    "bio_17",  # precipitation of driest quarter (summer drought)
    "bio_4",  # temperature seasonality
    "bio_15",  # precipitation seasonality
    "bio_5",  # max temperature of warmest month
    "bio_6",  # min temperature of coldest month
    "bio_14",  # precipitation of driest month
    "bio_2",  # mean diurnal range (average monthly max - min temperature)
    "bio_3",  # isothermality (see note above)
    "bio_7",  # temperature annual range (warmest month max - coldest month min)
    "bio_8",  # mean temp of wettest quarter
    "bio_9",  # mean temp of driest quarter
    "bio_10",  # mean temp of warmest quarter
    "bio_11",  # mean temp of coldest quarter
    "bio_13",  # precipitation of wettest month
    "bio_16",  # precipitation of wettest quarter
    "bio_18",  # precipitation of warmest quarter
    "bio_19",  # precipitation of coldest quarter
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
