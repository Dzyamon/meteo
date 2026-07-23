from __future__ import annotations

import logging

from lightgbm import LGBMRegressor

from meteo.config import get_settings, load_locations
from meteo.storage.timescale import TimescaleStore
from meteo_model.correct.dataset import FEATURE_COLUMNS, build_training_frame
from meteo_model.correct.storage import save_model
from meteo_model.schemas import CORRECTION_TARGETS

logger = logging.getLogger(__name__)


def train_location(location_id: str, db: TimescaleStore) -> dict:
    settings = get_settings()
    pairs = db.fetch_nwp_training_pairs(location_id, settings.gfs_model)
    stats = {"location_id": location_id, "pairs": len(pairs), "trained": []}

    if len(pairs) < settings.bias_correction_min_samples:
        logger.info(
            "Skipping %s: %s/%s training pairs (cold start, correction will pass through)",
            location_id,
            len(pairs),
            settings.bias_correction_min_samples,
        )
        return stats

    x_by_target, y_by_target, _ = build_training_frame(pairs)

    for target in CORRECTION_TARGETS:
        x = x_by_target[target]
        y = y_by_target[target]
        if len(y) < settings.bias_correction_min_samples:
            logger.info("Skipping target %s for %s: only %s samples", target, location_id, len(y))
            continue

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
        bundle = {
            "model": model,
            "target": target,
            "feature_columns": FEATURE_COLUMNS,
            "n_samples": int(len(y)),
        }
        path = save_model(location_id, target, bundle)
        stats["trained"].append(target)
        logger.info("Trained %s bias model for %s (%s samples) -> %s", target, location_id, len(y), path)

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
