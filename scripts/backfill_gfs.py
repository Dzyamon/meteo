"""Backfill historical GFS forecasts into nwp_forecasts.

For each past cycle it stores forecast horizons whose valid_times you can pair
with backfilled observations, giving Approach 3 enough (nwp, obs) pairs to train.

Needs GRIB tooling -> run in the meteo-model container:
    docker compose -f docker-compose.yml -f docker-compose.model.yml run --rm \
        meteo-model python scripts/backfill_gfs.py --days 8 --horizons 24

NOTE: AWS noaa-gfs-bdp-pds retains only ~10 days of GFS. Older history needs a
different archive. This is slow (~1 request per variable per horizon).
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone

from meteo.config import load_locations
from meteo.storage.timescale import TimescaleStore
from meteo_model.sources.gfs import fetch_forecast

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_gfs")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=8, help="how many past days of cycles to fetch")
    parser.add_argument("--cycle", type=int, default=0, choices=[0, 6, 12, 18], help="GFS cycle hour (UTC)")
    parser.add_argument("--horizons", type=int, default=24, help="forecast hours per cycle")
    args = parser.parse_args()

    today = datetime.now(timezone.utc).replace(hour=args.cycle, minute=0, second=0, microsecond=0)
    db = TimescaleStore()
    total = 0
    try:
        for day_offset in range(1, args.days + 1):
            run_time = today - timedelta(days=day_offset)
            # fetch_forecast derives the cycle from `now`; +6h makes it resolve to run_time
            anchor = run_time + timedelta(hours=6)
            for loc in load_locations():
                resolved, rows = fetch_forecast(loc, anchor, args.horizons)
                n = db.upsert_nwp_forecasts([r.as_dict() for r in rows])
                total += n
                logger.info("Backfilled %s GFS rows for %s (run %s)", n, loc.id, resolved.isoformat())
    finally:
        db.close()
    logger.info("Done. %s nwp_forecast rows upserted.", total)


if __name__ == "__main__":
    main()
