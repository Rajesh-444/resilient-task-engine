from __future__ import annotations

import json
import re


class JobSafetyPolicy:
    """Safe-by-default validation for queued job names and payloads."""

    MAX_PAYLOAD_BYTES = 16_384
    MAX_DEPTH = 6
    NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
    SENSITIVE_KEYS = {"password", "passwd", "token", "access_token", "api_key", "secret", "private_key"}
    EXECUTION_KEYS = {"command", "shell", "shell_command", "script", "code", "eval", "exec"}

    def validate(self, name: str, payload: dict) -> None:
        if not self.NAME_PATTERN.fullmatch(name.strip()):
            raise ValueError("job name must match ^[a-z][a-z0-9_.-]{0,63}$")
        if not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object")
        encoded = json.dumps(payload, sort_keys=True).encode()
        if len(encoded) > self.MAX_PAYLOAD_BYTES:
            raise ValueError(f"payload exceeds {self.MAX_PAYLOAD_BYTES} bytes")
        self._inspect(payload, depth=0)

    def _inspect(self, value, *, depth: int) -> None:
        if depth > self.MAX_DEPTH:
            raise ValueError(f"payload nesting exceeds {self.MAX_DEPTH} levels")
        if isinstance(value, dict):
            for key, child in value.items():
                normalized = str(key).lower().replace("-", "_")
                if normalized in self.SENSITIVE_KEYS:
                    raise ValueError(f"sensitive payload key is not allowed: {key}")
                if normalized in self.EXECUTION_KEYS:
                    raise ValueError(f"arbitrary execution payload key is not allowed: {key}")
                self._inspect(child, depth=depth + 1)
        elif isinstance(value, list):
            for child in value:
                self._inspect(child, depth=depth + 1)

