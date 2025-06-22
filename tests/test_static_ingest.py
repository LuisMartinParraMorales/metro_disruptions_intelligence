from pathlib import Path

import pandas as pd

from metro_disruptions_intelligence.etl.static_ingest import ingest_static_gtfs


def test_static_ingest(tmp_path):
    output_dir = tmp_path / "processed" / "static"
    parquet_path = ingest_static_gtfs(Path("sample_data/static"), output_dir)
    assert parquet_path.exists()
    df = pd.read_parquet(parquet_path)
    assert len(df) > 0

