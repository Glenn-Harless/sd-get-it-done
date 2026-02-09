"""Orchestrator: ingest → transform → export."""

from __future__ import annotations

import sys
import time

from pipeline.ingest import ingest
from pipeline.transform import transform
from pipeline.validate import validate


def main() -> None:
    force = "--force" in sys.argv
    t0 = time.time()

    print("=" * 60)
    print("Get It Done 311 Pipeline")
    print("=" * 60)

    print("\n── Step 1: Ingest ──")
    paths = ingest(force=force)
    print(f"  {len(paths)} files ready\n")

    print("── Step 2: Transform ──")
    transform()

    print("\n── Step 3: Validate ──")
    validate()

    elapsed = time.time() - t0
    print(f"\nPipeline complete in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
