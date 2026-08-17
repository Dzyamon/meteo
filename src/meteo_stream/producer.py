from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone

from meteo.clients.weather import OpenMeteoClient
from meteo.config import get_settings, load_locations
from meteo.storage.bronze import BronzeStore
from meteo_stream.kafka_client import create_producer, publish_event
from meteo_stream.schemas import ObservationBatchEvent, row_to_message
from meteo_stream.topics import OBSERVATIONS

logger = logging.getLogger(__name__)


def poll_and_publish(once: bool = False) -> None:
    settings = get_settings()
    locations = load_locations()
    open_meteo = OpenMeteoClient()
    bronze = BronzeStore()
    producer = create_producer()

    try:
        while True:
            cycle_started = datetime.now(timezone.utc)
            for location in locations:
                payload, rows = open_meteo.fetch_current_and_recent(
                    location,
                    lookback_hours=settings.ingest_lookback_hours,
                )
                bronze.save_json("open_meteo", location.id, payload)

                event = ObservationBatchEvent(
                    event_id=str(uuid.uuid4()),
                    location_id=location.id,
                    source="open_meteo",
                    ingested_at=cycle_started,
                    observations=[row_to_message(row) for row in rows],
                    raw_payload=payload,
                )
                publish_event(
                    producer,
                    settings.kafka_topic_observations,
                    key=location.id,
                    event=event.model_dump(mode="json"),
                )
                logger.info(
                    "Published %s observations for %s to %s",
                    len(rows),
                    location.id,
                    settings.kafka_topic_observations,
                )

            if once:
                break

            logger.info("Sleeping %s seconds until next poll", settings.producer_poll_interval_seconds)
            time.sleep(settings.producer_poll_interval_seconds)
    finally:
        producer.close()
        open_meteo.close()


def main() -> None:
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Poll Open-Meteo and publish to Kafka")
    parser.add_argument("--once", action="store_true", help="Run one poll cycle and exit")
    args = parser.parse_args()
    poll_and_publish(once=args.once)


if __name__ == "__main__":
    main()
