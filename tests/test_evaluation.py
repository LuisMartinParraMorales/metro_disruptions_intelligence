import pandas as pd

from metro_disruptions_intelligence.evaluation import build_events, evaluate_scores, label_scores


def test_label_scores() -> None:
    scores = pd.DataFrame({
        "ts": [0, 60, 120],
        "anomaly_score": [0.1, 0.5, 0.2],
        "anomaly_flag": [0, 1, 0],
    })
    alerts = pd.DataFrame({
        "alert_entity_id": ["a"],
        "active_period_start": [60],
        "active_period_end": [180],
        "cause": [1],
    })
    events = build_events(alerts)
    labels = label_scores(scores, events, lead_time=60)
    assert list(labels) == [1, 1, 1]


def test_evaluate_scores_runs() -> None:
    scores = pd.DataFrame({
        "ts": [0, 60, 120],
        "anomaly_score": [0.1, 0.5, 0.2],
        "anomaly_flag": [0, 1, 0],
    })
    alerts = pd.DataFrame({
        "alert_entity_id": ["a"],
        "active_period_start": [60],
        "active_period_end": [180],
        "cause": [1],
    })
    events = build_events(alerts)
    metrics = evaluate_scores(scores, events, k=1, lead_time=60)
    assert set(metrics.keys()) == {"lead_time_roc_auc", "precision_at_k", "mttd", "fpr"}
