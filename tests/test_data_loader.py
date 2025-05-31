# tests/test_data_loader.py

from metro_disruptions_intelligence.data_loader import (
    DATA_DIR,
    ALERTS_DIR,
    list_all_alert_files,
)
from pathlib import Path

def test_sample_alert_file_exists(tmp_path, monkeypatch):
    """
    Temporarily override DATA_DIR so that our test sees data/raw/ instead
    of the big OneDrive folder.
    """
    # monkeypatch the environment variable to point at our `data/` folder
    monkeypatch.setenv("METRO_DATA_DIR", str(Path(__file__).parents[1] / "data"))
    
    # Re‐import the loader so that it picks up the new env var
    import importlib
    loader = importlib.reload(__import__('metro_disruptions_intelligence.data_loader', fromlist=['*']))

    # Now `ALERTS_DIR` should be something like .../data/raw/
    assert loader.ALERTS_DIR.exists()
    files = loader.list_all_alert_files()
    assert len(files) == 1
    assert files[0].name == "sample_alert.json"

