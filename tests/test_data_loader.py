# tests/test_data_loader.py

from pathlib import Path


def test_sample_alert_file_exists(tmp_path, monkeypatch):
    """
    Temporarily override DATA_DIR so that our test sees data/raw/ instead
    of the big OneDrive folder.
    """
    # monkeypatch the environment variable to point at our `data/` folder
    monkeypatch.setenv("METRO_DATA_DIR", str(Path(__file__).parents[1] / "data"))

    # Import the loader after setting the environment variable so it picks up the new path
    import importlib

    loader = importlib.reload(importlib.import_module("metro_disruptions_intelligence.data_loader"))

    # Now `ALERTS_DIR` should be something like .../data/raw/
    assert loader.ALERTS_DIR.exists()
    files = loader.list_all_alert_files()
    assert len(files) == 1
    assert files[0].name == "sample_alert.json"


def test_sample_trip_update_file_exists(monkeypatch):
    """Ensure the sample trip update data file can be found."""
    monkeypatch.setenv("METRO_DATA_DIR", str(Path(__file__).parents[1] / "data"))
    import importlib

    loader = importlib.reload(importlib.import_module("metro_disruptions_intelligence.data_loader"))
    assert loader.TRIP_UPDATES_DIR.exists()
    files = loader.list_all_trip_update_files()
    assert len(files) == 1
    assert files[0].name == "sample_trip_update.json"


def test_sample_vehicle_positions_file_exists(monkeypatch):
    """Ensure the sample vehicle positions data file can be found."""
    monkeypatch.setenv("METRO_DATA_DIR", str(Path(__file__).parents[1] / "data"))
    import importlib

    loader = importlib.reload(importlib.import_module("metro_disruptions_intelligence.data_loader"))
    assert loader.VEHICLE_POSITIONS_DIR.exists()
    files = loader.list_all_vehicle_position_files()
    assert len(files) == 1
    assert files[0].name == "sample_vehicles_position.json"


def test_load_config_from_local_file(tmp_path, monkeypatch):
    """Verify `_load_config` reads from a local YAML file when no env var is set."""
    monkeypatch.delenv("METRO_DATA_DIR", raising=False)
    cfg_dir = Path(__file__).parents[1] / "config"
    cfg_dir.mkdir(exist_ok=True)
    local_file = cfg_dir / "local.yaml"
    local_file.write_text(f"data_dir: '{tmp_path}'\n", encoding="utf-8")

    import importlib

    loader = importlib.reload(importlib.import_module("metro_disruptions_intelligence.data_loader"))
    assert tmp_path == loader.DATA_DIR
    assert loader.ALERTS_DIR.parent.parent == tmp_path
    local_file.unlink()


def test_load_config_from_default_file(tmp_path, monkeypatch):
    """Verify `_load_config` uses default.yaml when no other sources exist."""
    monkeypatch.delenv("METRO_DATA_DIR", raising=False)
    default_file = Path(__file__).parents[1] / "config" / "default.yaml"
    orig = default_file.read_text(encoding="utf-8")
    default_file.write_text(f"data_dir: '{tmp_path}'\n", encoding="utf-8")

    import importlib

    loader = importlib.reload(importlib.import_module("metro_disruptions_intelligence.data_loader"))
    assert tmp_path == loader.DATA_DIR
    default_file.write_text(orig, encoding="utf-8")
