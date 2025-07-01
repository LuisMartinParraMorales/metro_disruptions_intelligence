"""Core utilities for :mod:`metro_disruptions_intelligence`."""

from __future__ import annotations


def greet(name: str = "World") -> str:
    """Return a friendly greeting message.

    Parameters
    ----------
    name:
        Name of the person to greet. Defaults to ``"World"``.

    Returns
    -------
    str
        A greeting string including ``name``.
    """

    return f"Hello, {name}!"


__all__ = ["greet"]

