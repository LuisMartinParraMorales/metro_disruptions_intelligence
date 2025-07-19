"""Grid search utilities for :class:`StreamingIForestDetector`."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd
import pytz
import yaml

from .streaming_iforest import StreamingIForestDetector

logger = logging.getLogger(__name__)


_DEF_RESULTS = Path("data/working_data/tuning_results.csv")
_DEF_CACHE = Path("data/working_data/cache")
_DEF_BEST = Path("iforest_best.yaml")


def _snapshot_path(root: Path, ts: int) -> Path:
    dt = datetime.fromtimestamp(ts, tz=pytz.UTC)
    return (
        root
        / f"year={dt.year:04d}"
        / f"month={dt.month:02d}"
        / f"day={dt.day:02d}"
        / f"stations_feats_{dt:%Y-%d-%m-%H-%M}.parquet"
    )


def _score_range(root: Path, config: dict | str | Path, start: datetime, end: datetime) -> pd.DataFrame:
    det = StreamingIForestDetector(config)
    start_ts = int(start.timestamp())
    end_ts = int(end.timestamp())
    rows: list[pd.DataFrame] = []
    for ts in range(start_ts, end_ts, 60):
        path = _snapshot_path(root, ts)
        if not path.exists():
            continue
        df = pd.read_parquet(path)
        out = det.score_and_update(df)
        logger.info("processed %s -> %d rows", path, len(out))
        if not out.empty:
            rows.append(out)
    if rows:
        return pd.concat(rows, ignore_index=True)
    return pd.DataFrame(
        columns=["ts", "stop_id", "direction_id", "anomaly_score", "anomaly_flag", "shap_top3_json"]
    )


def _evaluate(df: pd.DataFrame) -> tuple[float, float]:
    if df.empty:
        return 0.0, 0.0
    return float(df["anomaly_score"].mean()), float(df["anomaly_flag"].mean())


def run_grid_search(
    processed_root: Path,
    grid_yaml: Path | str,
    start: datetime,
    end: datetime,
    cache_dir: Path | None = None,
    *,
    results_csv: Path | None = None,
    best_yaml: Path | None = None,
) -> pd.DataFrame:
    """Run a serial grid search over StreamingIForestDetector parameters."""
    cache_dir = Path(cache_dir or _DEF_CACHE)
    results_csv = Path(results_csv or _DEF_RESULTS)
    best_yaml = Path(best_yaml or _DEF_BEST)

    with open(grid_yaml, encoding="utf-8") as f:
        grid_cfg = yaml.safe_load(f)

    keys = list(grid_cfg.keys())
    values_list = [v if isinstance(v, list) else [v] for v in grid_cfg.values()]
    combos = list(product(*values_list))[:16]

    rows = []
    cache_dir.mkdir(parents=True, exist_ok=True)
    for combo in combos:
        params = dict(zip(keys, combo))
        key_str = json.dumps(params, sort_keys=True)
        cache_file = cache_dir / (hashlib.md5(key_str.encode()).hexdigest() + ".parquet")
        if cache_file.exists():
            scores = pd.read_parquet(cache_file)
        else:
            scores = _score_range(processed_root, params, start, end)
            scores.to_parquet(cache_file, index=False)
        auc, prec = _evaluate(scores)
        rows.append({**params, "lead_time_roc_auc": auc, "precision_at_k": prec})

    df = pd.DataFrame(rows)
    results_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(results_csv, index=False)

    if not df.empty:
        best = df.sort_values("lead_time_roc_auc", ascending=False).iloc[0]

        def _py(v: object) -> object:
            if isinstance(v, (np.floating, np.integer)):
                return v.item()
            return v

        best_params = {k: _py(best[k]) for k in keys}
        with open(best_yaml, "w", encoding="utf-8") as f:
            yaml.safe_dump(best_params, f)
    return df
