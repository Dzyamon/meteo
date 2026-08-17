"""Vercel Python entrypoint — exposes the FastAPI ASGI app for all routes.

vercel.json rewrites every path to this function, so the single app serves the
dashboard, the read endpoints, and the /cron/* jobs.
"""
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))
# Make config files resolvable regardless of the function's working directory.
os.environ.setdefault("LOCATIONS_CONFIG", os.path.join(_ROOT, "config", "locations.yaml"))
os.environ.setdefault("ALERTS_CONFIG", os.path.join(_ROOT, "config", "alerts.yaml"))

from meteo.api.main import app  # noqa: E402  (path setup must run first)
