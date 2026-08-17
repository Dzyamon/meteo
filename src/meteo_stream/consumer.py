from __future__ import annotations

import logging

from pydantic import ValidationError

from meteo.config import get_settings, load_locations
from meteo.features.engineering import build_feature_rows
from meteo.storage.timescale import TimescaleStore
from meteo_stream.kafka_client import create_consumer
from meteo_stream.schemas import ObservationBatchEvent

logger = logging.getLogger(__name__)


def process_batch(event: ObservationBatchEvent, db: TimescaleStore) -> dict:
    rows = [obs.to_row() for obs in event.observations]
    observation_count = db.upsert_observations(rows)

    history = db.fetch_observations_for_features(event.location_id, source=event.source)
    feature_rows = build_feature_rows(history)
    feature_count = db.upsert_features(feature_rows)

    return {
        "event_id": event.event_id,
        "location_id": event.location_id,
        "observations": observation_count,
        "features": feature_count,
    }


def consume_forever() -> None:
    settings = get_settings()
    known_locations = {loc.id for loc in load_locations()}
    consumer = create_consumer()
    db = TimescaleStore()

    logger.info(
        "Listening on %s (group=%s, bootstrap=%s)",
        settings.kafka_topic_observations,
        settings.kafka_consumer_group,
        settings.kafka_bootstrap_servers,
    )

    try:
        for message in consumer:
            try:
                event = ObservationBatchEvent.model_validate(message.value)
                if event.location_id not in known_locations:
                    logger.warning("Skipping unknown location: %s", event.location_id)
                    consumer.commit()
                    continue

                stats = process_batch(event, db)
                consumer.commit()
                logger.info("Processed batch: %s", stats)
            except ValidationError as exc:
                logger.error("Invalid message at offset %s: %s", message.offset, exc)
                consumer.commit()
            except Exception:
                logger.exception("Failed to process message at offset %s", message.offset)
    finally:
        consumer.close()
        db.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    consume_forever()


if __name__ == "__main__":
    main()
