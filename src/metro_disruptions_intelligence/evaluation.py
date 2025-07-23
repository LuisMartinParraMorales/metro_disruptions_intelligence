"""Utilities for evaluating anomaly scores against alerts."""

import numpy as np
import pandas as pd


def _roc_auc_score(y_true: np.ndarray, scores: np.ndarray) -> float:
    """Compute ROC-AUC without external dependencies."""
    order = np.argsort(scores)
    y_true = y_true[order]
    ranks = np.argsort(order)
    n_pos = y_true.sum()
    n_neg = len(y_true) - n_pos
    if n_pos == 0 or n_neg == 0:
        return 0.0
    sum_pos = (ranks[y_true == 1] + 1).sum()
    auc = (sum_pos - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)
    return float(auc)


def build_events(alerts: pd.DataFrame) -> pd.DataFrame:
    """Return unique alert events."""
    cols = ["alert_entity_id", "active_period_start", "active_period_end", "cause"]
    return alerts[cols].drop_duplicates()


def label_scores(scores: pd.DataFrame, events: pd.DataFrame, lead_time: int = 900) -> pd.Series:
    """Label score rows as true (1) if within event lead window."""
    if events.empty:
        return pd.Series([0] * len(scores), index=scores.index)
    starts = events["active_period_start"].to_numpy()
    ends = events["active_period_end"].to_numpy()
    labels = []
    for ts in scores["ts"].to_numpy():
        match = False
        for s, e in zip(starts, ends):
            if s - lead_time <= ts <= e:
                match = True
                break
        labels.append(1 if match else 0)
    return pd.Series(labels, index=scores.index)


def precision_at_k(scored: pd.DataFrame, k: int = 50) -> float:
    """Return the mean precision of the top-k scores per day."""
    if scored.empty:
        return 0.0
    scored = scored.copy()
    scored["date"] = pd.to_datetime(scored["ts"], unit="s", utc=True).dt.date
    precs = []
    for _d, grp in scored.groupby("date"):
        top = grp.nlargest(k, "anomaly_score")
        precs.append(top["label"].mean())
    return float(np.nanmean(precs)) if precs else 0.0


def mean_time_to_detection(scored: pd.DataFrame, events: pd.DataFrame) -> float:
    """Return the average minutes from event start to first detection."""
    deltas = []
    for _, ev in events.iterrows():
        detections = scored[
            (scored["ts"] >= ev["active_period_start"]) & (scored["ts"] <= ev["active_period_end"])
        ]
        if detections.empty:
            continue
        first = detections.sort_values("ts").iloc[0]["ts"]
        deltas.append((first - ev["active_period_start"]) / 60.0)
    return float(np.mean(deltas)) if deltas else float("nan")


def evaluate_scores(
    scored: pd.DataFrame, events: pd.DataFrame, k: int = 50, lead_time: int = 900
) -> dict:
    """Compute evaluation metrics for ``scored`` given ground truth ``events``."""
    scored = scored.copy()
    scored["label"] = label_scores(scored, events, lead_time)
    auc = 0.0
    if scored["label"].nunique() > 1:
        auc = _roc_auc_score(scored["label"].to_numpy(), scored["anomaly_score"].to_numpy())
    prec = precision_at_k(scored, k)
    mttd = mean_time_to_detection(scored, events)
    fp = ((scored["label"] == 0) & (scored["anomaly_flag"] == 1)).sum()
    tn = ((scored["label"] == 0) & (scored["anomaly_flag"] == 0)).sum()
    fpr = float(fp / (fp + tn)) if (fp + tn) else 0.0
    return {"lead_time_roc_auc": auc, "precision_at_k": prec, "mttd": mttd, "fpr": fpr}
