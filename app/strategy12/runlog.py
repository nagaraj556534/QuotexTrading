from __future__ import annotations
import os
import json
from datetime import datetime
from typing import Any


def _default(o: Any):
    # Helper for json serialization
    if isinstance(o, set):
        return list(o)
    return str(o)


class RunLogger:
    """Lightweight run artifact logger for Strategy 12.
    Creates a timestamped directory and writes JSON/JSONL files
    so users can inspect what happened during the pipeline run.
    """

    def __init__(self, base_dir: str = "artifacts/strategy12/runs", prefix: str | None = None):
        ts = prefix or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_dir = os.path.join(base_dir, ts)
        os.makedirs(self.run_dir, exist_ok=True)

    def path(self, name: str) -> str:
        if not name.endswith(".json") and not name.endswith(".jsonl"):
            name = f"{name}.json"
        return os.path.join(self.run_dir, name)

    def write_json(self, name: str, data: Any) -> None:
        p = self.path(name if name.endswith(".json") else f"{name}.json")
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=_default)

    def append_jsonl(self, name: str, data: Any) -> None:
        p = self.path(name if name.endswith(".jsonl") else f"{name}.jsonl")
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps(data, default=_default))
            f.write("\n")

    def info(self) -> str:
        return self.run_dir

