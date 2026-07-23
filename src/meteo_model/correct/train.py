from __future__ import annotations

import logging

import numpy as np
from lightgbm import LGBMRegressor

from meteo.config import get_settings, load_locations
from meteo.storage.timescale import TimescaleStore
from meteo_model.correct.dataset import FEATURE_COLUMNS, build_training_frame
from meteo_model.correct.storage import save_model
from meteo_model.schemas import CORRECTION_TARGETS

logger = logging.getLogger(__name__)

MIN_VAL_SAMPLES = 20  # need at least this many held-out points to judge skill


def _fit(x: np.ndarray, y: np.ndarray) -> LGBMRegressor:
    model = LGBMRegressor(
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        verbose=-1,
    )
    model.fit(x, y)
    return model


def _split_by_time(pairs: list[dict], holdout_fraction: float) -> tuple[list[dict], list[dict]]:
    """Temporal split: oldest (1-frac) trains, most-recent frac validates (no leakage)."""
    ordered = sorted(pairs, key=lambda r: r["valid_time"])
    cut = int(len(ordered) * (1 - holdout_fraction))
    return ordered[:cut], ordered[cut:]


def train_location(location_id: str, db: TimescaleStore) -> dict:
    settings = get_settings()
    pairs = db.fetch_nwp_training_pairs(location_id, settings.gfs_model)
    stats = {"location_id": location_id, "pairs": len(pairs), "trained": [], "skipped": []}

    if len(pairs) < settings.bias_correction_min_samples:
        logger.info(
            "Skipping %s: %s/%s training pairs (cold start, correction will pass through)",
            location_id,
            len(pairs),
            settings.bias_correction_min_samples,
        )
        return stats

    train_pairs, val_pairs = _split_by_time(pairs, settings.validation_holdout_fraction)
    x_tr, y_tr, _ = build_training_frame(train_pairs)
    x_va, y_va, _ = build_training_frame(val_pairs)
    x_all, y_all, _ = build_training_frame(pairs)

    for target in CORRECTION_TARGETS:
        if len(y_all[target]) < settings.bias_correction_min_samples:
            logger.info("Skipping target %s for %s: only %s samples", target, location_id, len(y_all[target]))
            continue

        # Out-of-sample validation: residual y already encodes obs-nwp, so
        # |pred - y| is the corrected error and |y| is the raw-GFS error.
        val = None
        if len(y_tr[target]) >= 1 and len(y_va[target]) >= MIN_VAL_SAMPLES:
            probe = _fit(x_tr[target], y_tr[target])
            pred = probe.predict(x_va[target])
            mae_corrected = float(np.mean(np.abs(pred - y_va[target])))
            mae_raw = float(np.mean(np.abs(y_va[target])))
            val = {
                "n_val": int(len(y_va[target])),
                "val_mae_raw": round(mae_raw, 3),
                "val_mae_corrected": round(mae_corrected, 3),
                "val_improvement": round(mae_raw - mae_corrected, 3),
            }

        # Promotion gate: never deploy a correction that loses to raw GFS out-of-sample.
        if val and val["val_mae_corrected"] >= val["val_mae_raw"]:
            logger.warning(
                "NOT deploying %s for %s: correction worse than raw on holdout (%.3f >= %.3f)",
                target, location_id, val["val_mae_corrected"], val["val_mae_raw"],
            )
            stats["skipped"].append({"target": target, **val})
            continue

        final = _fit(x_all[target], y_all[target])
        bundle = {
            "model": final,
            "target": target,
            "feature_columns": FEATURE_COLUMNS,
            "n_samples": int(len(y_all[target])),
            **(val or {"n_val": 0}),
        }
        save_model(location_id, target, bundle)
        stats["trained"].append({"target": target, "n": int(len(y_all[target])), **(val or {})})
        if val:
            logger.info(
                "Trained %s for %s: holdout MAE raw=%.3f -> corrected=%.3f (improvement %.3f, n_val=%s)",
                target, location_id, val["val_mae_raw"], val["val_mae_corrected"],
                val["val_improvement"], val["n_val"],
            )
        else:
            logger.info("Trained %s for %s (%s samples; validation skipped, too few holdout points)",
                        target, location_id, len(y_all[target]))

    return stats


def train_all() -> list[dict]:
    locations = load_locations()
    db = TimescaleStore()
    try:
        return [train_location(loc.id, db) for loc in locations]
    finally:
        db.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    results = train_all()
    logger.info("Training complete: %s", results)


if __name__ == "__main__":
    main()
