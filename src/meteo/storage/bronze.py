from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.client import Config

from meteo.config import get_settings


class BronzeStore:
    """Persist raw API payloads (bronze layer)."""

    def __init__(self) -> None:
        self.settings = get_settings()
        if not self.settings.use_local_bronze:
            self._s3 = boto3.client(
                "s3",
                endpoint_url=self.settings.minio_endpoint,
                aws_access_key_id=self.settings.minio_access_key,
                aws_secret_access_key=self.settings.minio_secret_key,
                config=Config(signature_version="s3v4"),
            )
        else:
            self._s3 = None
            self.settings.local_bronze_path.mkdir(parents=True, exist_ok=True)

    def save_json(self, source: str, location_id: str, payload: dict) -> str:
        now = datetime.now(timezone.utc)
        key = (
            f"{source}/{location_id}/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"{now.strftime('%Y%m%dT%H%M%SZ')}.json"
        )
        body = json.dumps(payload, default=str).encode("utf-8")

        if self._s3 is not None:
            self._s3.put_object(
                Bucket=self.settings.minio_bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
            )
            return f"s3://{self.settings.minio_bucket}/{key}"

        path = self.settings.local_bronze_path / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return str(path)
