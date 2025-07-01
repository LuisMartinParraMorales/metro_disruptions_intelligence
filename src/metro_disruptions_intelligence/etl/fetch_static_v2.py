from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import requests
from tqdm import tqdm
import zipfile
import shutil


API_ROOT = "https://api.transport.nsw.gov.au/v2/gtfs/schedule"


def _stream_download(url: str, headers: dict[str, str], dest: Path) -> None:
    """Download ``url`` to ``dest`` streaming in 1MB chunks."""
    with requests.get(url, headers=headers, stream=True) as r:
        if r.status_code == 404:
            print("operator slug not found")
            raise SystemExit(1)
        if r.status_code == 403:
            print("invalid API key")
            raise SystemExit(1)
        if not r.ok:
            print(f"HTTP error {r.status_code}")
            raise SystemExit(1)
        ctype = r.headers.get("Content-Type", "")
        if not ctype.startswith("application/zip"):
            print("unexpected content type")
            raise SystemExit(1)
        total = int(r.headers.get("Content-Length", 0))
        with open(dest, "wb") as fh:
            with tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
            ) as bar:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fh.write(chunk)
                        bar.update(len(chunk))


def _extract_zip(zip_path: Path, out_dir: Path, force: bool) -> int:
    """Extract ``zip_path`` into ``out_dir`` respecting ``force`` flag."""
    count = 0
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            dest = out_dir / info.filename
            dest.parent.mkdir(parents=True, exist_ok=True)
            mtime = datetime(*info.date_time).timestamp()
            if dest.exists() and not force:
                same_size = dest.stat().st_size == info.file_size
                if same_size and int(dest.stat().st_mtime) == int(mtime):
                    continue
            with zf.open(info) as src, open(dest, "wb") as dst:
                shutil.copyfileobj(src, dst)
            os.utime(dest, (mtime, mtime))
            count += 1
    return count


def download_and_extract(
    api_key: str,
    operator: str,
    out_root: Path,
    *,
    force: bool = False,
    skip_if_exists: bool = True,
) -> Path:
    """Download the static feed for ``operator`` and extract it."""
    out_root = Path(out_root)
    date_str = date.today().isoformat()
    feeds_dir = out_root / "static_feeds"
    feeds_dir.mkdir(parents=True, exist_ok=True)
    zip_path = feeds_dir / f"{date_str}_{operator}.zip"

    if not (skip_if_exists and zip_path.exists()):
        url = f"{API_ROOT}/{operator}"
        headers = {"Authorization": f"apikey {api_key}"}
        _stream_download(url, headers, zip_path)
    else:
        logging.info("Using cached file %s", zip_path)

    out_dir = out_root / "static"
    out_dir.mkdir(parents=True, exist_ok=True)
    extracted = _extract_zip(zip_path, out_dir, force)
    size_mb = zip_path.stat().st_size / 1024 / 1024
    print(
        f"\u2713 downloaded {size_mb:.0f} MB  \u2794  {zip_path} ({extracted} files extracted)"
    )
    return out_dir


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download gtfs-static v2 feed")
    parser.add_argument("--api_key", required=True, help="TfNSW API key")
    parser.add_argument("--operator", required=True, help="Operator slug")
    parser.add_argument("--out_root", type=Path, default=Path("data"), help="Output root directory")
    parser.add_argument("--force", action="store_true", help="Force overwrite existing files")
    parser.add_argument("--skip_if_exists", action="store_true", help="Skip download if zip already exists")
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = _parse_args(argv)
    download_and_extract(
        args.api_key,
        args.operator,
        args.out_root,
        force=args.force,
        skip_if_exists=args.skip_if_exists,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    main()
