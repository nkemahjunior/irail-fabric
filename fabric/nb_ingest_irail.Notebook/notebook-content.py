# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fa0b35b2-b3c3-4aa8-8659-791f2aee89db",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "3d9a52a4-37cb-4643-af42-3d3bee62d970",
# META       "known_lakehouses": [
# META         {
# META           "id": "fa0b35b2-b3c3-4aa8-8659-791f2aee89db"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import json, logging, os, sys, time
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    stream=sys.stdout, force=True)
log = logging.getLogger("ingest_irail")

cfg = notebookutils.variableLibrary.getLibrary("vl_irail")
BASE_URL = cfg.irail_base_url
USER_AGENT = cfg.user_agent
STATION_IDS = [s.strip() for s in cfg.station_ids.split(",") if s.strip()]
PAUSE_SECONDS = float(cfg.pause_seconds)

RAW_ROOT = Path("/lakehouse/default/Files/raw")
log.info("Config loaded: %d stations", len(STATION_IDS))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def make_session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=2,
                  status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch(session: requests.Session, endpoint: str, params: dict) -> dict:
    response = session.get(f"{BASE_URL}/{endpoint}",
                           params={**params, "format": "json", "lang": "en"}, timeout=30)
    response.raise_for_status()
    return response.json()


def save_raw(data: dict, endpoint: str, name: str, run_ts: datetime) -> Path:
    folder = RAW_ROOT / endpoint / f"date={run_ts:%Y-%m-%d}"
    folder.mkdir(parents=True, exist_ok=True)
    final_path = folder / f"{name}_{run_ts:%Y%m%dT%H%M%SZ}.json"
    tmp_path = folder / f".tmp_{final_path.name}"
    tmp_path.write_text(json.dumps(data))
    os.replace(tmp_path, final_path)
    return final_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

run_ts = datetime.now(timezone.utc)
session = make_session()
failures = []

for station_id in STATION_IDS:
    try:
        data = fetch(session, "liveboard", {"id": station_id, "arrdep": "departure"})
        save_raw(data, "liveboard", station_id, run_ts)
        name = data.get("stationinfo", {}).get("name", "?")
        count = len(data.get("departures", {}).get("departure", []))
        log.info("%s (%s): %d departures", name, station_id, count)
    except Exception:
        log.exception("Liveboard failed for %s", station_id)
        failures.append(station_id)
    time.sleep(PAUSE_SECONDS)

try:
    data = fetch(session, "disturbances", {})
    save_raw(data, "disturbances", "disturbances", run_ts)
    log.info("Disturbances: %d active", len(data.get("disturbance", [])))
except Exception:
    log.exception("Disturbances failed")
    failures.append("disturbances")

if failures:
    raise RuntimeError(f"{len(failures)} request(s) failed: {failures}")
log.info("All %d requests succeeded", len(STATION_IDS) + 1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
