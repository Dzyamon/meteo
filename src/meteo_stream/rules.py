from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel

from meteo.config import get_settings
from meteo_stream.schemas import ObservationMessage

Operator = Literal["gt", "gte", "lt", "lte"]


class AlertRule(BaseModel):
    id: str
    metric: str
    operator: Operator
    threshold: float
    severity: str = "warning"
    message: str


@dataclass
class AlertMatch:
    rule: AlertRule
    value: float
    observation_time: str


def load_alert_rules(config_path: Path | None = None) -> list[AlertRule]:
    settings = get_settings()
    path = config_path or settings.alerts_config
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return [AlertRule.model_validate(item) for item in data.get("rules", [])]


def _compare(value: float, operator: Operator, threshold: float) -> bool:
    if operator == "gt":
        return value > threshold
    if operator == "gte":
        return value >= threshold
    if operator == "lt":
        return value < threshold
    return value <= threshold


def evaluate_rules(
    observation: ObservationMessage,
    rules: list[AlertRule],
) -> list[AlertMatch]:
    matches: list[AlertMatch] = []
    payload = observation.model_dump()
    for rule in rules:
        raw_value = payload.get(rule.metric)
        if raw_value is None:
            continue
        value = float(raw_value)
        if _compare(value, rule.operator, rule.threshold):
            matches.append(
                AlertMatch(
                    rule=rule,
                    value=value,
                    observation_time=observation.time.isoformat(),
                )
            )
    return matches


def latest_observation(observations: list[ObservationMessage]) -> ObservationMessage | None:
    if not observations:
        return None
    return max(observations, key=lambda obs: obs.time)
