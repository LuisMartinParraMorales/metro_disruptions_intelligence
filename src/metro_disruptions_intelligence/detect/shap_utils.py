"""Helper utilities for SHAP-like explanations."""

from __future__ import annotations


def top_n_tree_shap(model, x: dict[str, float], n: int = 3) -> list[tuple[str, float]]:
    """Return top-N feature contributions using a simple ablation approach."""
    base = model.score_one(x)
    scores: list[tuple[str, float]] = []
    for k in x:
        x0 = dict(x)
        x0[k] = 0
        diff = base - model.score_one(x0)
        scores.append((k, diff))
    scores.sort(key=lambda kv: abs(kv[1]), reverse=True)
    return scores[:n]
