import pandas as pd

import pytest

from metro_disruptions_intelligence.evaluation import (
    build_events,
    evaluate_scores,
    label_delays,
    label_scores,
)


@pytest.fixture
def delay_scores() -> pd.DataFrame:
    """Fixture with known delays and anomaly flags."""
    return pd.DataFrame(
        {
            "ts": [0, 60, 120, 180, 240],
            "anomaly_score": [0.05, 0.1, 0.9, 0.8, 0.2],
            "anomaly_flag": [0, 0, 1, 1, 0],
            "arrival_delay_t": [0, 130, 140, 0, 0],
            "departure_delay_t": [0, 0, 0, 150, 0],
        }
    )


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


def test_evaluate_scores_threshold() -> None:
    scores = pd.DataFrame({
        "ts": [0, 60, 120],
        "anomaly_score": [0.1, 0.5, 0.2],
        "anomaly_flag": [0, 1, 0],
        "arrival_delay_t": [0, 130, 0],
        "departure_delay_t": [0, 0, 0],
    })
    metrics = evaluate_scores(scores, None, k=1, delay_threshold=120)
    assert set(metrics.keys()) == {"lead_time_roc_auc", "precision_at_k", "mttd", "fpr"}


def test_label_delays_thresholds(delay_scores: pd.DataFrame) -> None:
    """label_delays should honour different thresholds and missing columns."""
    assert label_delays(delay_scores, 120).tolist() == [0, 1, 1, 1, 0]
    assert label_delays(delay_scores, 160).tolist() == [0, 0, 0, 0, 0]

    missing = delay_scores.drop(columns=["arrival_delay_t", "departure_delay_t"])
    assert label_delays(missing, 120).tolist() == [0, 0, 0, 0, 0]


def test_evaluate_scores_delay_metrics(delay_scores: pd.DataFrame) -> None:
    """evaluate_scores returns expected metrics for the fixture."""
    metrics = evaluate_scores(delay_scores, None, k=1, delay_threshold=120)
    assert metrics["lead_time_roc_auc"] == pytest.approx(0.5)
    assert metrics["precision_at_k"] == pytest.approx(1.0)
    assert metrics["mttd"] == pytest.approx(1.0)
    assert metrics["fpr"] == pytest.approx(0.0)
