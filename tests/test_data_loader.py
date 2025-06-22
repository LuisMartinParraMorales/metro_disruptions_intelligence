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
