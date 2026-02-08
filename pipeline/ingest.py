"""Download Get It Done 311 CSVs from data.sandiego.gov."""

from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path

import httpx

BASE_URL = "https://seshat.datasd.org/get_it_done_reports"
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# Open requests + closed by year (2016 through current year)
CURRENT_YEAR = datetime.now().year

SOURCES: dict[str, str] = {
    "open": f"{BASE_URL}/get_it_done_requests_open_datasd.csv",
}
for year in range(2016, CURRENT_YEAR + 1):
    SOURCES[f"closed_{year}"] = (
        f"{BASE_URL}/get_it_done_requests_closed_{year}_datasd.csv"
    )


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download(name: str, url: str, *, force: bool = False) -> Path:
    """Download a single CSV. Skips if file exists and force=False."""
    dest = RAW_DIR / f"{name}.csv"
    if dest.exists() and not force:
        print(f"  [skip] {name} (already exists, {dest.stat().st_size:,} bytes)")
        return dest

    print(f"  [download] {name} ...")
    with httpx.stream("GET", url, follow_redirects=True, timeout=300) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=1 << 20):
                f.write(chunk)
    print(f"  [done] {name} -> {dest.stat().st_size:,} bytes")
    return dest


def ingest(*, force: bool = False) -> list[Path]:
    """Download all source CSVs. Returns list of downloaded file paths."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, url in SOURCES.items():
        try:
            paths.append(download(name, url, force=force))
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                print(f"  [warn] {name}: 403 forbidden, skipping")
            else:
                raise
    return paths


if __name__ == "__main__":
    ingest()
