from __future__ import annotations

import io
import zipfile
from datetime import date
from pathlib import Path

import pytest

import sys
import types
import importlib.util

sys.modules.setdefault("duckdb", types.ModuleType("duckdb"))
sys.modules.setdefault("pyarrow", types.ModuleType("pyarrow"))
requests_mod = types.ModuleType("requests")
requests_mod.get = lambda *a, **k: None
sys.modules.setdefault("requests", requests_mod)
tqdm_mod = types.ModuleType("tqdm")
def fake_tqdm(*args, **kwargs):
    class Dummy:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

        def update(self, n):
            pass

    return Dummy()

tqdm_mod.tqdm = fake_tqdm
sys.modules.setdefault("tqdm", tqdm_mod)

spec = importlib.util.spec_from_file_location(
    "fetch_static_v2",
    Path(__file__).parents[1] / "src" / "metro_disruptions_intelligence" / "etl" / "fetch_static_v2.py",
)
fetch_static_v2 = importlib.util.module_from_spec(spec)
assert spec.loader
spec.loader.exec_module(fetch_static_v2)


class FakeResp:
    def __init__(self, data: bytes):
        self.status_code = 200
        self.headers = {
            "Content-Type": "application/zip",
            "Content-Length": str(len(data)),
        }
        self._data = data
        self.ok = True

    def iter_content(self, chunk_size: int = 1024 * 1024):
        for i in range(0, len(self._data), chunk_size):
            yield self._data[i : i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass


def make_zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("stops.txt", "id,name\n1,A\n")
        zf.writestr("routes.txt", "id,name\n1,R\n")
    return buf.getvalue()


def test_download_and_extract(monkeypatch, tmp_path):
    data = make_zip_bytes()
    monkeypatch.setattr(fetch_static_v2, "date", type("D", (), {"today": staticmethod(lambda: date(2025, 3, 31))}))
    monkeypatch.setattr(fetch_static_v2.requests, "get", lambda *a, **k: FakeResp(data))
    out = fetch_static_v2.download_and_extract("key", "metro", tmp_path, skip_if_exists=False)
    zip_path = tmp_path / "static_feeds" / "2025-03-31_metro.zip"
    assert zip_path.exists()
    extracted = sorted((tmp_path / "static").glob("*.txt"))
    assert {p.name for p in extracted} == {"stops.txt", "routes.txt"}
    assert out == tmp_path / "static"
