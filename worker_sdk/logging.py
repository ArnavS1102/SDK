from __future__ import annotations
import json
import sys
import time
import traceback
from typing import Any, Dict, Optional


class StructuredLogger:
    """Simple structured JSON logger used across SDK and services."""

    def __init__(self, name: str = "sdk", level: str = "INFO"):
        self.name = name
        self.level = level.upper()
        self._level_order = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}

    # ----------------------------------------------------------------------
    # Core logging method
    # ----------------------------------------------------------------------
    def _log(self, level: str, msg: str, extra: Optional[Dict[str, Any]] = None):
        """Internal helper: format and print a JSON log line."""
        if self._level_order.get(level, 100) < self._level_order.get(self.level, 20):
            return  # skip lower-level logs

        try:
            record = {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "level": level,
                "logger": self.name,
                "msg": str(msg),
            }

            if extra and isinstance(extra, dict):
                for k, v in extra.items():
                    # Avoid overwriting core keys
                    if k not in record:
                        record[k] = v

            # Ensure JSON serializable output
            line = json.dumps(record, ensure_ascii=False)
            print(line, file=sys.stdout, flush=True)

        except Exception as e:
            # Never crash the app due to logging errors
            print(f"[logger-error] failed to log: {e}", file=sys.stderr, flush=True)

    # ----------------------------------------------------------------------
    # Public convenience methods
    # ----------------------------------------------------------------------
    def info(self, msg: str, extra: Optional[Dict[str, Any]] = None):
        self._log("INFO", msg, extra)

    def warning(self, msg: str, extra: Optional[Dict[str, Any]] = None):
        self._log("WARNING", msg, extra)

    def error(self, msg: Any, extra: Optional[Dict[str, Any]] = None):
        # Auto-handle exception objects
        if isinstance(msg, BaseException):
            err_str = f"{type(msg).__name__}: {msg}"
            tb = traceback.format_exc()
            extra = dict(extra or {}, traceback=tb)
            self._log("ERROR", err_str, extra)
        else:
            self._log("ERROR", msg, extra)

    def debug(self, msg: str, extra: Optional[Dict[str, Any]] = None):
        self._log("DEBUG", msg, extra)

    # ----------------------------------------------------------------------
    # Context binding
    # ----------------------------------------------------------------------
    def bind(self, **context: Any) -> "StructuredLogger":
        """
        Return a lightweight copy of the logger with context permanently attached.
        Example:
            log = get_logger("runner").bind(job_id="abc123", task_id="t5")
        """
        child = StructuredLogger(name=self.name, level=self.level)
        child._bound = getattr(self, "_bound", {}).copy()
        child._bound.update(context)
        original_log = child._log

        def _log_with_context(level, msg, extra=None):
            combined = dict(getattr(child, "_bound", {}))
            if extra:
                combined.update(extra)
            original_log(level, msg, combined)

        child._log = _log_with_context
        return child


# ----------------------------------------------------------------------
# Module-level logger registry (one logger per name)
# ----------------------------------------------------------------------

_loggers: Dict[str, StructuredLogger] = {}


def get_logger(name: str = "sdk", level: str = "INFO") -> StructuredLogger:
    """Get or create a logger for the given name."""
    if name not in _loggers:
        _loggers[name] = StructuredLogger(name=name, level=level)
    return _loggers[name]


# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------

__all__ = ["StructuredLogger", "get_logger"]
