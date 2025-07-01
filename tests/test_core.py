"""Tests for the :mod:`metro_disruptions_intelligence.core` module."""

from metro_disruptions_intelligence import core


def test_greet() -> None:
    """Ensure that :func:`core.greet` returns the expected greeting."""

    assert core.greet("Tester") == "Hello, Tester!"
