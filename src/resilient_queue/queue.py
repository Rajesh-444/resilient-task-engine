from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .guardrails import JobSafetyPolicy


Clock = Callable[[], float]


@dataclass(frozen=True)
class Job:
    id: int
    name: str
    payload: dict
    status: str
    attempts: int
    max_attempts: int
    available_at: float
    lease_until: float | None
    worker_id: str | None
    last_error: str | None
    idempotency_key: str | None


class SQLiteQueue:
    """A small at-least-once queue with explicit state transitions."""

    def __init__(
        self,
        path: str | Path,
        clock: Clock = time.time,
        safety_policy: JobSafetyPolicy | None = None,
    ):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self.safety_policy = safety_policy or JobSafetyPolicy()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                PRAGMA journal_mode = WAL;
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    available_at REAL NOT NULL,
                    lease_until REAL,
                    worker_id TEXT,
                    last_error TEXT,
                    idempotency_key TEXT UNIQUE,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS jobs_claimable
                    ON jobs(status, available_at, lease_until, id);
                """
            )

    @staticmethod
    def _job(row: sqlite3.Row) -> Job:
        return Job(
            id=row["id"],
            name=row["name"],
            payload=json.loads(row["payload_json"]),
            status=row["status"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            available_at=row["available_at"],
            lease_until=row["lease_until"],
            worker_id=row["worker_id"],
            last_error=row["last_error"],
            idempotency_key=row["idempotency_key"],
        )

    def get(self, job_id: int) -> Job:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(f"job {job_id} not found")
        return self._job(row)

    def enqueue(
        self,
        name: str,
        payload: dict,
        *,
        max_attempts: int = 3,
        delay_seconds: float = 0,
        idempotency_key: str | None = None,
    ) -> Job:
        if not name.strip():
            raise ValueError("name is required")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if delay_seconds < 0:
            raise ValueError("delay_seconds cannot be negative")
        self.safety_policy.validate(name, payload)
        now = self.clock()
        with self._connect() as connection:
            try:
                cursor = connection.execute(
                    """
                    INSERT INTO jobs (
                        name, payload_json, status, max_attempts, available_at,
                        idempotency_key, created_at, updated_at
                    ) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?)
                    """,
                    (
                        name.strip(),
                        json.dumps(payload, sort_keys=True),
                        max_attempts,
                        now + delay_seconds,
                        idempotency_key,
                        now,
                        now,
                    ),
                )
                job_id = cursor.lastrowid
            except sqlite3.IntegrityError:
                if idempotency_key is None:
                    raise
                row = connection.execute(
                    "SELECT * FROM jobs WHERE idempotency_key = ?", (idempotency_key,)
                ).fetchone()
                return self._job(row)
        return self.get(job_id)

    def claim(self, worker_id: str, lease_seconds: float = 30) -> Job | None:
        if not worker_id.strip():
            raise ValueError("worker_id is required")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now = self.clock()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT * FROM jobs
                WHERE
                    (status IN ('queued', 'retry') AND available_at <= ?)
                    OR (status = 'running' AND lease_until <= ?)
                ORDER BY available_at, id
                LIMIT 1
                """,
                (now, now),
            ).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return None
            connection.execute(
                """
                UPDATE jobs
                SET status = 'running', attempts = attempts + 1, worker_id = ?,
                    lease_until = ?, updated_at = ?
                WHERE id = ?
                """,
                (worker_id, now + lease_seconds, now, row["id"]),
            )
            updated = connection.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone()
            connection.execute("COMMIT")
        return self._job(updated)

    def complete(self, job_id: int, worker_id: str) -> Job:
        return self._owned_transition(
            job_id,
            worker_id,
            """
            UPDATE jobs
            SET status = 'completed', lease_until = NULL, worker_id = NULL, updated_at = ?
            WHERE id = ? AND status = 'running' AND worker_id = ?
            """,
            (self.clock(), job_id, worker_id),
        )

    def fail(
        self,
        job_id: int,
        worker_id: str,
        error: str,
        *,
        base_backoff_seconds: float = 5,
        max_backoff_seconds: float = 300,
    ) -> Job:
        if base_backoff_seconds < 0 or max_backoff_seconds < 0:
            raise ValueError("backoff values cannot be negative")
        current = self.get(job_id)
        if current.status != "running" or current.worker_id != worker_id:
            raise RuntimeError("job is not owned by this worker")
        now = self.clock()
        if current.attempts >= current.max_attempts:
            status, available_at = "dead_letter", current.available_at
        else:
            status = "retry"
            backoff = min(base_backoff_seconds * (2 ** (current.attempts - 1)), max_backoff_seconds)
            available_at = now + backoff
        return self._owned_transition(
            job_id,
            worker_id,
            """
            UPDATE jobs
            SET status = ?, available_at = ?, lease_until = NULL, worker_id = NULL,
                last_error = ?, updated_at = ?
            WHERE id = ? AND status = 'running' AND worker_id = ?
            """,
            (status, available_at, error[:1000], now, job_id, worker_id),
        )

    def _owned_transition(self, job_id: int, worker_id: str, sql: str, values: tuple) -> Job:
        with self._connect() as connection:
            cursor = connection.execute(sql, values)
            if cursor.rowcount != 1:
                raise RuntimeError("job is not owned by this worker")
        return self.get(job_id)

    def counts(self) -> dict[str, int]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM jobs GROUP BY status ORDER BY status"
            ).fetchall()
        return {row["status"]: row["count"] for row in rows}

