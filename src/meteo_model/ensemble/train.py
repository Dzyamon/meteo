from __future__ import annotations

import logging

import numpy as np
from lightgbm import LGBMRegressor

from meteo.config import get_settings, load_locations
from meteo.storage.timescale import TimescaleStore
from meteo_model.ensemble.dataset import (
    ENSEMBLE_MEMBERS,
    ENSEMBLE_TARGETS,
    build_training_frame,
    feature_columns,
)
from meteo_model.ensemble.storage import save_ensemble

logger = logging.getLogger(__name__)

MIN_VAL_SAMPLES = 20
# The ensemble must clear the training gate, but on its own sample count.
MIN_ENSEMBLE_SAMPLES = 100


def _fit(x: np.ndarray, y: np.ndarray) -> LGBMRegressor:
    model = LGBMRegressor(
        n_estimators=250,
        learning_rate=0.05,
        num_leaves=15,  # few members -> keep it shallow to avoid overfitting
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.9,
        verbose=-1,
    )
    model.fit(x, y)
    return model


def _best_member_mae(x_val: np.ndarray, y_val: np.ndarray) -> tuple[float, str]:
    """Best single-member MAE on the holdout (the bar the ensemble must beat)."""
    best_mae, best_name = float("inf"), "none"
    for i, member in enumerate(ENSEMBLE_MEMBERS):
        col = x_val[:, i]
        mask = ~np.isnan(col)
        if mask.sum() < MIN_VAL_SAMPLES:
            continue
        mae = float(np.mean(np.abs(col[mask] - y_val[mask])))
        if mae < best_mae:
            best_mae, best_name = mae, member
    return best_mae, best_name


def train_location(location_id: str, db: TimescaleStore) -> dict:
    settings = get_settings()
    rows = db.fetch_ensemble_training_rows(location_id, ENSEMBLE_MEMBERS)
    stats = {"location_id": location_id, "trained": [], "skipped": []}

    for target in ENSEMBLE_TARGETS:
        X, y, times = build_training_frame(rows, target)
        if len(y) < MIN_ENSEMBLE_SAMPLES:
            stats["skipped"].append({"target": target, "reason": f"only {len(y)} samples"})
            continue

        cut = int(len(y) * (1 - settings.validation_holdout_fraction))  # times are sorted
        x_tr, y_tr, x_va, y_va = X[:cut], y[:cut], X[cut:], y[cut:]
        if len(y_va) < MIN_VAL_SAMPLES:
            stats["skipped"].append({"target": target, "reason": f"holdout too small ({len(y_va)})"})
            continue

        probe = _fit(x_tr, y_tr)
        ens_mae = float(np.mean(np.abs(probe.predict(x_va) - y_va)))
        best_mae, best_name = _best_member_mae(x_va, y_va)

        # Gate: deploy the ensemble only if it beats the best single member out-of-sample.
        if ens_mae >= best_mae:
            logger.warning(
                "NOT deploying ensemble %s for %s: %.3f >= best member %s %.3f",
                target, location_id, ens_mae, best_name, best_mae,
            )
            stats["skipped"].append(
                {"target": target, "reason": "loses to best member",
                 "ensemble_mae": round(ens_mae, 3), "best_member": best_name, "best_member_mae": round(best_mae, 3)}
            )
            continue

        final = _fit(X, y)
        save_ensemble(location_id, target, {
            "model": final,
            "target": target,
            "members": ENSEMBLE_MEMBERS,
            "feature_columns": feature_columns(target),
            "n_samples": int(len(y)),
            "val_ensemble_mae": round(ens_mae, 3),
            "val_best_member": best_name,
            "val_best_member_mae": round(best_mae, 3),
        })
        stats["trained"].append(
            {"target": target, "n": int(len(y)), "ensemble_mae": round(ens_mae, 3),
             "best_member": best_name, "best_member_mae": round(best_mae, 3),
             "improvement": round(best_mae - ens_mae, 3)}
        )
        logger.info(
            "Trained ensemble %s for %s: holdout MAE %.3f vs best member %s %.3f (n=%s)",
            target, location_id, ens_mae, best_name, best_mae, len(y),
        )

    return stats


def train_all() -> list[dict]:
    db = TimescaleStore()
    try:
        return [train_location(loc.id, db) for loc in load_locations()]
    finally:
        db.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger.info("Ensemble training complete: %s", train_all())


if __name__ == "__main__":
    main()
