from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

@dataclass
class Ctx:
    """
    Execution context shared by the runner and service hooks.

    S3-only MVP:
      - The runner decides what objects to put here.
      - Keep types as Any to avoid over-engineering.
    """
    config: Dict[str, Any]
    storage: Any   # e.g., an object/module exposing read/write/exists the runner provides
    sqs: Any       # runner-provided SQS client/adapter
    db: Any        # runner-provided DB client/adapter
    logger: Any    # runner-provided logger

class WorkerHooks:
    """
    Services implement ONLY:
      - load_pipeline(ctx)
      - process(ctx, msg) -> {"result": ..., "metrics": {...}, optional "fanout": [...]}
    """
    model: Any = None

    def load_pipeline(self, ctx: Ctx) -> Any:
        raise NotImplementedError

    def process(self, ctx: Ctx, msg: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


__all__ = ["Ctx", "WorkerHooks"]
