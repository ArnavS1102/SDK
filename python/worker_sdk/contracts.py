# worker_sdk/contracts.py
"""
SDK Contracts (MVP)
- No business logic here.
- Just shared types, a simple context container, and the hooks interface.

Services will:
  - import WorkerHooks and subclass it
  - implement: load_pipeline(ctx), process(ctx, msg), publish_next(ctx, msg, outputs)

The runner will:
  - build a Ctx (config + IO adapters + logger)
  - call hooks.load_pipeline(ctx) once at startup
  - for each message: hooks.process(...) -> write outputs; hooks.publish_next(...) -> enqueue children
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

# ---------------------------
# Public type aliases
# ---------------------------

TaskMessage = Dict[str, Any]      # Normalized message dict delivered to hooks
ProcessOutputs = Dict[str, Any]   # {"result": ..., "metrics": {...}, "aux": {...}} from hooks.process


# ---------------------------
# IO & Logger protocols (duck-typed)
# ---------------------------

@runtime_checkable
class StorageProto(Protocol):
    """Minimal storage interface used by the runner (and only by the runner)."""
    def read(self, uri: str) -> bytes: ...
    def write(self, prefix: str, filename: str, data: bytes, content_type: Optional[str] = None) -> str: ...
    def exists(self, uri_or_prefix: str) -> bool: ...


@runtime_checkable
class SQSProto(Protocol):
    """SQS adapter used by the runner."""
    def receive(self, queue_url: str, max_messages: int, wait_seconds: int) -> List[Dict[str, Any]]: ...
    def delete(self, queue_url: str, receipt_handle: str) -> None: ...
    # The runner may also use a helper like io_sqs.send_messages_batch(...) for publishing


@runtime_checkable
class DBProto(Protocol):
    """DB adapter used by the runner for task/job status."""
    def claim_task(self, task_id: str) -> bool: ...
    def update_status(self, task_id: str, status: str) -> None: ...


@runtime_checkable
class LoggerProto(Protocol):
    """Structured logger used everywhere."""
    def info(self, msg: str, extra: Optional[Dict[str, Any]] = None) -> None: ...
    def warning(self, msg: str, extra: Optional[Dict[str, Any]] = None) -> None: ...
    def error(self, msg: str, extra: Optional[Dict[str, Any]] = None) -> None: ...
    def debug(self, msg: str, extra: Optional[Dict[str, Any]] = None) -> None: ...


# ---------------------------
# Context object (passed to hooks and runner internals)
# ---------------------------

@dataclass
class Ctx:
    """
    Execution context shared by the runner and service hooks.
    - config: merged env + YAML (simple dict)
    - storage/sqs/db/logger: concrete adapters provided by the runner
    """
    config: Dict[str, Any]
    storage: StorageProto
    sqs: SQSProto
    db: DBProto
    logger: LoggerProto


# ---------------------------
# Hooks contract (to be implemented by services)
# ---------------------------

class WorkerHooks:
    """
    Base contract for service hooks.

    Implement these three methods in your service:

      1) load_pipeline(ctx) -> Any
         - Called once at worker startup.
         - Load heavy models/resources and return an object you'll reuse in `process`.

      2) process(ctx, msg: TaskMessage) -> ProcessOutputs
         - Called per message.
         - Run your business/ML logic.
         - Return a dict that can include:
             {
               "result": <dict|bytes>,            # runner writes to result.json|png|mp4
               "metrics": <dict>,                 # runner writes to metrics.json
               "aux": { "path/rel.ext": bytes }   # optional extra artifacts under output_prefix
             }

      3) publish_next(ctx, msg: TaskMessage, outputs: ProcessOutputs) -> List[TaskMessage]
         - Return child messages for the next step (or [] to end the branch).
         - The runner will publish them.
    """

    # Loaded object holder (runner sets this after calling load_pipeline)
    # Services may access `self.model` or `self.pipeline` in process().
    model: Any = None  # optional convenience attribute

    def load_pipeline(self, ctx: Ctx) -> Any:
        """Load heavy resources once (models, tokenizers, etc.)."""
        raise NotImplementedError("Service must implement load_pipeline(ctx)")

    def process(self, ctx: Ctx, msg: TaskMessage) -> ProcessOutputs:
        """Run the step's logic for a single message and return outputs."""
        raise NotImplementedError("Service must implement process(ctx, msg)")

    def publish_next(self, ctx: Ctx, msg: TaskMessage, outputs: ProcessOutputs) -> List[TaskMessage]:
        """Describe fan-out for the next step; return [] if final."""
        return []


# ---------------------------
# Public API surface
# ---------------------------

__all__ = [
    "Ctx",
    "WorkerHooks",
    "TaskMessage",
    "ProcessOutputs",
    "StorageProto",
    "SQSProto",
    "DBProto",
    "LoggerProto",
]

