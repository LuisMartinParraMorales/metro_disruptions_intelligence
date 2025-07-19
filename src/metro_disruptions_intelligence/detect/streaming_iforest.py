"""Streaming Isolation Forest detector using River."""

from __future__ import annotations

import json
import logging
import pickle
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from river import anomaly, compose

from ..utils_gtfsrt import sydney_time
from .shap_utils import top_n_tree_shap

logger = logging.getLogger(__name__)

BAD_STOP_IDS = {"204472", "2155270"}


@dataclass
class IForestConfig:
    """Hyper-parameters for :class:`StreamingIForestDetector`."""

    n_trees: int = 100
    height: int = 10
    subsample_size: int = 256
    window_size: int = 10_000
    threshold_quantile: float = 0.97
    warmup_days: int = 4


class StreamingIForestDetector:
    """Online anomaly detector based on Half-Space Trees."""

    def __init__(self, config: IForestConfig | dict | str | Path) -> None:
        """Initialise the detector with ``config``."""
        if isinstance(config, (str, Path)):
            cfg_dict = self._load_yaml(Path(config))
            self.config = IForestConfig(**cfg_dict)
        elif isinstance(config, dict):
            self.config = IForestConfig(**config)
        else:
            self.config = config

        self._build_pipeline()
        self.scores: deque[float] = deque(maxlen=self.config.window_size)
        self.n_obs = 0
        self.current_service_day: Any = None
        self.feature_cols: list[str] | None = None

    @staticmethod
    def _load_yaml(path: Path) -> dict:
        import yaml

        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _build_pipeline(self) -> None:
        self.pipeline = compose.Pipeline(
            # ("scale", preprocessing.MinMaxScaler()),
            anomaly.HalfSpaceTrees(
                n_trees=self.config.n_trees,
                height=self.config.height,
                window_size=self.config.window_size,
            )
        )

    # ------------------------------------------------------------------
    def _service_day(self, ts: int) -> Any:
        dt = sydney_time(ts)
        if dt.hour < 3:
            dt -= pd.Timedelta(days=1)
        return dt.date()

    def _maybe_reset(self, ts: int) -> None:
        sd = self._service_day(ts)
        if self.current_service_day is None:
            self.current_service_day = sd
        elif sd != self.current_service_day:
            logger.info("Service day boundary reached – resetting state")
            self._build_pipeline()
            self.scores.clear()
            self.n_obs = 0
            self.current_service_day = sd

    # ------------------------------------------------------------------
    def score_and_update(self, df_minute: pd.DataFrame, *, explain: bool = False) -> pd.DataFrame:
        """Score each snapshot in ``df_minute`` and update the model."""
        if df_minute.empty:
            return pd.DataFrame(
                columns=[
                    "ts",
                    "stop_id",
                    "direction_id",
                    "anomaly_score",
                    "anomaly_flag",
                    "shap_top3_json",
                ]
            )

        ts = int(df_minute["snapshot_timestamp"].iloc[0])
        self._maybe_reset(ts)

        df = df_minute.copy()
        df = df[~df["stop_id"].astype(str).isin(BAD_STOP_IDS)]
        df.fillna(0, inplace=True)
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        df = df[(df[numeric_cols].abs().sum(axis=1) != 0)]

        if df.empty:
            return pd.DataFrame(
                columns=[
                    "ts",
                    "stop_id",
                    "direction_id",
                    "anomaly_score",
                    "anomaly_flag",
                    "shap_top3_json",
                ]
            )

        if self.feature_cols is None:
            exclude = {"snapshot_timestamp", "stop_id", "direction_id", "local_dt"}
            self.feature_cols = [c for c in df.columns if c not in exclude]

        rows = []
        for _, row in df.iterrows():
            x = {k: row[k] for k in self.feature_cols}
            score = float(self.pipeline.score_one(x))
            self.scores.append(score)
            self.n_obs += 1

            flag = 0
            if self.n_obs >= self.config.window_size:
                threshold = float(np.quantile(self.scores, self.config.threshold_quantile))
                if score > threshold:
                    flag = 1

            shap_json = None
            if explain:
                shap_json = json.dumps(top_n_tree_shap(self.pipeline[-1], x, n=3))

            rows.append({
                "ts": ts,
                "stop_id": row["stop_id"],
                "direction_id": row["direction_id"],
                "anomaly_score": score,
                "anomaly_flag": flag if self.n_obs >= self.config.window_size else 0,
                "shap_top3_json": shap_json,
            })
            self.pipeline.learn_one(x)
        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    def save(self, path: str | Path) -> None:
        """Persist the detector to ``path``."""
        state = {
            "config": self.config.__dict__,
            "pipeline": self.pipeline,
            "scores": list(self.scores),
            "n_obs": self.n_obs,
            "current_service_day": self.current_service_day,
            "feature_cols": self.feature_cols,
        }
        with open(path, "wb") as f:
            pickle.dump(state, f)

    @classmethod
    def load(cls, path: str | Path) -> StreamingIForestDetector:
        """Load a persisted detector from ``path``."""
        with open(path, "rb") as f:
            state = pickle.load(f)
        obj = cls(state["config"])
        obj.pipeline = state["pipeline"]
        obj.scores = deque(state["scores"], maxlen=obj.config.window_size)
        obj.n_obs = state["n_obs"]
        obj.current_service_day = state["current_service_day"]
        obj.feature_cols = state["feature_cols"]
        return obj
