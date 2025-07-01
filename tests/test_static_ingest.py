from pathlib import Path

import pandas as pd
import pytest

from metro_disruptions_intelligence.etl.static_ingest import ingest_static_gtfs


def test_static_ingest(tmp_path):
    output_dir = tmp_path / "processed" / "static"
    gtfs_path = Path("data/static")
    if not gtfs_path.exists():
        pytest.skip("Static GTFS data not available")
    parquet_path = ingest_static_gtfs(gtfs_path, output_dir)
    assert parquet_path.exists()
    df = pd.read_parquet(parquet_path)
    assert len(df) > 0

