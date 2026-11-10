import tempfile
import unittest
from pathlib import Path

from resilient_queue import SQLiteQueue, Worker


class ManualClock:
    def __init__(self, value: float = 1_000):
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.clock = ManualClock()
        self.queue = SQLiteQueue(Path(self.temp.name) / "queue.db", clock=self.clock)

    def tearDown(self):
        self.temp.cleanup()

    def test_idempotency_key_returns_original_job(self):
        first = self.queue.enqueue("sync", {"version": 1}, idempotency_key="customer-42")
        second = self.queue.enqueue("sync", {"version": 2}, idempotency_key="customer-42")
        self.assertEqual(first.id, second.id)
        self.assertEqual(second.payload, {"version": 1})

    def test_delayed_job_is_not_claimed_early(self):
        self.queue.enqueue("sync", {}, delay_seconds=10)
        self.assertIsNone(self.queue.claim("worker-a"))
        self.clock.advance(10)
        self.assertIsNotNone(self.queue.claim("worker-a"))

    def test_expired_lease_is_reclaimed(self):
        job = self.queue.enqueue("sync", {})
        first_claim = self.queue.claim("worker-a", lease_seconds=5)
        self.assertEqual(first_claim.id, job.id)
        self.assertIsNone(self.queue.claim("worker-b"))
        self.clock.advance(5)
        second_claim = self.queue.claim("worker-b")
        self.assertEqual(second_claim.id, job.id)
        self.assertEqual(second_claim.attempts, 2)

    def test_failure_retries_with_exponential_backoff(self):
        job = self.queue.enqueue("sync", {}, max_attempts=3)
        self.queue.claim("worker-a")
        failed = self.queue.fail(job.id, "worker-a", "temporary", base_backoff_seconds=4)
        self.assertEqual(failed.status, "retry")
        self.assertEqual(failed.available_at, self.clock() + 4)
        self.assertIsNone(self.queue.claim("worker-a"))

    def test_last_attempt_moves_to_dead_letter(self):
        job = self.queue.enqueue("sync", {}, max_attempts=1)
        self.queue.claim("worker-a")
        failed = self.queue.fail(job.id, "worker-a", "permanent")
        self.assertEqual(failed.status, "dead_letter")
        self.assertEqual(failed.last_error, "permanent")

    def test_wrong_worker_cannot_complete_job(self):
        job = self.queue.enqueue("sync", {})
        self.queue.claim("worker-a")
        with self.assertRaisesRegex(RuntimeError, "not owned"):
            self.queue.complete(job.id, "worker-b")


class WorkerTests(unittest.TestCase):
    def test_worker_records_handler_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            queue = SQLiteQueue(Path(directory) / "queue.db")
            job = queue.enqueue("explode", {}, max_attempts=1)

            def explode(_payload):
                raise ValueError("bad input")

            result = Worker(queue, "worker-a", {"explode": explode}).run_once()
            self.assertEqual(result.id, job.id)
            self.assertEqual(result.status, "dead_letter")
            self.assertIn("ValueError: bad input", result.last_error)


if __name__ == "__main__":
    unittest.main()

