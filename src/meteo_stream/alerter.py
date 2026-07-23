from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

import httpx
from pydantic import ValidationError

from meteo.config import get_settings, load_locations
from meteo.storage.timescale import TimescaleStore
from meteo_stream.kafka_client import create_consumer, create_producer, publish_event
from meteo_stream.rules import evaluate_rules, latest_observation, load_alert_rules
from meteo_stream.schemas import ObservationBatchEvent
from meteo_stream.topics import ALERTS

logger = logging.getLogger(__name__)


def _build_alert_event(
    location_id: str,
    match,
    alert_id: str,
    triggered_at: datetime,
) -> dict:
    return {
        "alert_id": alert_id,
        "rule_id": match.rule.id,
        "location_id": location_id,
        "severity": match.rule.severity,
        "metric": match.rule.metric,
        "value": match.value,
        "threshold": match.rule.threshold,
        "message": match.rule.message,
        "triggered_at": triggered_at.isoformat(),
        "observation_time": match.observation_time,
    }


def _notify_webhook(url: str, payload: dict) -> None:
    try:
        response = httpx.post(url, json=payload, timeout=10.0)
        response.raise_for_status()
    except Exception:
        logger.exception("Failed to send alert webhook")


def process_alerts(event: ObservationBatchEvent, db: TimescaleStore, producer) -> list[dict]:
    settings = get_settings()
    rules = load_alert_rules()
    latest = latest_observation(event.observations)
    if latest is None:
        return []

    fired: list[dict] = []
    for match in evaluate_rules(latest, rules):
        if db.recent_alert_exists(
            event.location_id,
            match.rule.id,
            settings.alert_cooldown_seconds,
        ):
            logger.info(
                "Skipping cooldown alert %s for %s",
                match.rule.id,
                event.location_id,
            )
            continue

        alert_id = str(uuid.uuid4())
        triggered_at = datetime.now(timezone.utc)
        alert_event = _build_alert_event(event.location_id, match, alert_id, triggered_at)

        db.save_alert(
            {
                "id": alert_id,
                "triggered_at": triggered_at,
                "location_id": event.location_id,
                "rule_id": match.rule.id,
                "severity": match.rule.severity,
                "metric": match.rule.metric,
                "value": match.value,
                "threshold": match.rule.threshold,
                "message": match.rule.message,
                "observation_time": latest.time,
            }
        )
        publish_event(producer, settings.kafka_topic_alerts, event.location_id, alert_event)

        if settings.alert_webhook_url:
            _notify_webhook(settings.alert_webhook_url, alert_event)

        logger.warning("ALERT %s [%s] %s", match.rule.severity, event.location_id, match.rule.message)
        fired.append(alert_event)

    return fired


def consume_forever() -> None:
    settings = get_settings()
    known_locations = {loc.id for loc in load_locations()}
    consumer = create_consumer(group_id=settings.kafka_consumer_group_alerts)
    producer = create_producer()
    db = TimescaleStore()

    logger.info(
        "Alert service listening on %s (group=%s), publishing to %s",
        settings.kafka_topic_observations,
        settings.kafka_consumer_group_alerts,
        settings.kafka_topic_alerts,
    )

    try:
        for message in consumer:
            try:
                event = ObservationBatchEvent.model_validate(message.value)
                if event.location_id not in known_locations:
                    consumer.commit()
                    continue

                fired = process_alerts(event, db, producer)
                consumer.commit()
                if fired:
                    logger.info("Fired %s alerts for %s", len(fired), event.location_id)
            except ValidationError as exc:
                logger.error("Invalid message at offset %s: %s", message.offset, exc)
                consumer.commit()
            except Exception:
                logger.exception("Failed to process alert at offset %s", message.offset)
    finally:
        consumer.close()
        producer.close()
        db.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    consume_forever()


if __name__ == "__main__":
    main()
