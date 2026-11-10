from __future__ import annotations

import argparse
from pathlib import Path

from .queue import SQLiteQueue
from .worker import Worker


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a durable queue demonstration")
    parser.add_argument("--database", type=Path, default=Path("queue.db"))
    args = parser.parse_args()

    queue = SQLiteQueue(args.database)
    queue.enqueue(
        "build_report",
        {"report": "weekly-quality-summary"},
        idempotency_key="demo-weekly-quality-summary",
    )

    def build_report(payload: dict) -> None:
        print(f"Building {payload['report']}")

    result = Worker(queue, "demo-worker", {"build_report": build_report}).run_once()
    print(f"Job {result.id}: {result.status}")
    print(f"Queue counts: {queue.counts()}")


if __name__ == "__main__":
    main()

