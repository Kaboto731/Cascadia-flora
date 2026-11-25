"""Model factory: one simple baseline and two flexible learners."""

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import PolynomialFeatures, StandardScaler

MODEL_NAMES = ("logistic", "rf", "xgboost")


def make_model(name: str, seed: int = 0, pos_weight: float = 1.0):
    """pos_weight = n_background / n_presence, used by XGBoost (the others balance internally)."""
    if name == "logistic":  # quadratic terms let it model "optimum" niches, like classic GLM SDMs
        return make_pipeline(
            StandardScaler(),
            PolynomialFeatures(2, include_bias=False),
            LogisticRegression(C=0.1, class_weight="balanced", max_iter=2000),
        )
    if name == "rf":
        return RandomForestClassifier(
            n_estimators=300,
            min_samples_leaf=5,
            class_weight="balanced_subsample",
            n_jobs=-1,
            random_state=seed,
        )
    if name == "xgboost":
        from xgboost import XGBClassifier

        return XGBClassifier(
            n_estimators=300,
            max_depth=3,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=5,
            scale_pos_weight=pos_weight,
            eval_metric="logloss",
            random_state=seed,
            n_jobs=2,
        )
    raise ValueError(f"Unknown model: {name}")
