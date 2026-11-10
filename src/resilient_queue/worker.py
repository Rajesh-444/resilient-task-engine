from __future__ import annotations

from collections.abc import Callable

from .queue import Job, SQLiteQueue


Handler = Callable[[dict], None]


class Worker:
    def __init__(self, queue: SQLiteQueue, worker_id: str, handlers: dict[str, Handler]):
        self.queue = queue
        self.worker_id = worker_id
        self.handlers = handlers

    def run_once(self) -> Job | None:
        job = self.queue.claim(self.worker_id)
        if job is None:
            return None
        handler = self.handlers.get(job.name)
        if handler is None:
            return self.queue.fail(job.id, self.worker_id, f"no handler registered for {job.name}")
        try:
            handler(job.payload)
        except Exception as error:  # worker boundary intentionally records handler failures
            return self.queue.fail(job.id, self.worker_id, f"{type(error).__name__}: {error}")
        return self.queue.complete(job.id, self.worker_id)

