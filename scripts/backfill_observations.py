"""Backfill historical observations from the Open-Meteo archive (ERA5).

Fills the `observations` table so Approach 3 bias correction has ground truth
to pair against historical GFS forecasts. Host-runnable (no GRIB tooling).

Usage:
    python scripts/backfill_observations.py --days 60
    python scripts/backfill_observations.py --start 2026-05-01 --end 2026-06-30
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta, timezone

from meteo.clients.weather import OpenMeteoClient
from meteo.config import load_locations
from meteo.storage.bronze import BronzeStore
from meteo.storage.timescale import TimescaleStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_observations")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=60, help="days back from today (ignored if --start given)")
    parser.add_argument("--start", type=str, help="ISO start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="ISO end date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    today = datetime.now(timezone.utc).date()
    end = date.fromisoformat(args.end) if args.end else today
    start = date.fromisoformat(args.start) if args.start else end - timedelta(days=args.days)

    client = OpenMeteoClient()
    bronze = BronzeStore()
    db = TimescaleStore()
    total = 0
    try:
        for loc in load_locations():
            payload, rows = client.fetch_archive(loc, start.isoformat(), end.isoformat())
            bronze.save_json("open_meteo_archive", loc.id, payload)
            n = db.upsert_observations(rows)
            total += n
            logger.info("Backfilled %s obs for %s (%s -> %s)", n, loc.id, start, end)
    finally:
        client.close()
        db.close()
    logger.info("Done. %s observations upserted.", total)


if __name__ == "__main__":
    main()
