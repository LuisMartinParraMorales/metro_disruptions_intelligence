# src/metro_disruptions_intelligence/data_loader.py

import os
import yaml
from pathlib import Path


def _load_config() -> dict:
    """
    Load configuration from (in order of priority)
      1) environment variable METRO_DATA_DIR
      2) config/local.yaml (if exists)
      3) config/default.yaml

    Returns a dict with at least the key 'data_dir'.
    """
    # 1) Check environment variable
    env_path = os.getenv("METRO_DATA_DIR")
    if env_path:
        return {"data_dir": env_path}

    # 2) Attempt to load local override
    local_cfg_path = Path(__file__).resolve().parents[2] / "config" / "local.yaml"
    if local_cfg_path.exists():
        with open(local_cfg_path, "r", encoding="utf-8") as f:
            local_cfg = yaml.safe_load(f)
        if local_cfg and "data_dir" in local_cfg and local_cfg["data_dir"]:
            return local_cfg

    # 3) Fallback to default.yaml
    default_cfg_path = Path(__file__).resolve().parents[2] / "config" / "default.yaml"
    if default_cfg_path.exists():
        with open(default_cfg_path, "r", encoding="utf-8") as f:
            default_cfg = yaml.safe_load(f)
        if default_cfg and "data_dir" in default_cfg and default_cfg["data_dir"]:
            return default_cfg

    raise RuntimeError(
        "Could not find data_dir. Please set the environment variable "
        "`METRO_DATA_DIR` or populate `config/local.yaml` with a valid path."
    )


# Load config at import time so we don’t repeatedly re‐read YAML
_cfg = _load_config()
DATA_DIR = Path(_cfg["data_dir"]).expanduser()

# Define the three subdirectories (relative to DATA_DIR)
ALERTS_DIR = DATA_DIR / "RAIL_RT_ALERTS" / "RAIL_RT_ALERTS"
TRIP_UPDATES_DIR = DATA_DIR / "RAIL_RT_TRIP_UPDATES" / "RAIL_RT_TRIP_UPDATES"
VEHICLE_POSITIONS_DIR = DATA_DIR / "RAIL_RT_VEHICLE_POSITIONS" / "RAIL_RT_VEHICLE_POSITIONS"


def list_all_alert_files() -> list[Path]:
    """
    Return a list of all JSON files under ALERTS_DIR.
    """
    return sorted(ALERTS_DIR.glob("*.json"))


def list_all_trip_update_files() -> list[Path]:
    """
    Return a list of all JSON files under TRIP_UPDATES_DIR.
    """
    return sorted(TRIP_UPDATES_DIR.glob("*.json"))


def list_all_vehicle_position_files() -> list[Path]:
    """
    Return a list of all JSON files under VEHICLE_POSITIONS_DIR.
    """
    return sorted(VEHICLE_POSITIONS_DIR.glob("*.json"))

