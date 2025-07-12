import pandas as pd
from pathlib import Path
from datetime import datetime
import pytz


def _write(path: Path, ts: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"snapshot_timestamp": [ts]}).to_parquet(path)


def test_filename_vs_epoch_offset(tmp_path: Path) -> None:
    london = pytz.timezone("Europe/London")
    pre_name = "trip_updates_2025-29-03-11-55.parquet"
    pre_ts = int(datetime(2025, 3, 29, 11, 55, tzinfo=pytz.UTC).timestamp())
    _write(tmp_path / pre_name, pre_ts)

    post_name = "trip_updates_2025-31-03-11-55.parquet"
    post_ts = int(datetime(2025, 3, 31, 11, 55, tzinfo=pytz.UTC).timestamp())
    _write(tmp_path / post_name, post_ts)

    offsets = []
    for fname in [pre_name, post_name]:
        part = fname.split("trip_updates_")[-1].replace(".parquet", "")
        local = london.localize(datetime.strptime(part, "%Y-%d-%m-%H-%M"))
        dt = local.astimezone(pytz.UTC)
        ts = int(pd.read_parquet(tmp_path / fname)["snapshot_timestamp"].min())
        offsets.append(int((ts - dt.timestamp()) // 3600))

    assert offsets == [0, 1]
