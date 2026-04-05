"""
Audit logger for dhan-nifty-mcp.
Every tool call is logged as a single JSON line.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


class AuditLogger:
    def __init__(self, config: dict):
        log_cfg = config.get("logging", {})
        log_dir = os.path.expanduser(log_cfg.get("dir", "~/.dhan-mcp/logs"))
        audit_file = log_cfg.get("audit_file", "trades.jsonl")

        Path(log_dir).mkdir(parents=True, exist_ok=True)
        self._path = os.path.join(log_dir, audit_file)

    def log(
        self,
        tool: str,
        params: dict,
        result: Any,
        mode: str,
        latency_ms: Optional[float] = None,
        error: Optional[str] = None,
    ):
        """Append a single audit entry."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "tool": tool,
            "mode": mode,
            "params": _safe_serialize(params),
            "result": _safe_serialize(result),
            "latency_ms": round(latency_ms, 1) if latency_ms else None,
            "error": error,
        }

        try:
            with open(self._path, "a") as f:
                f.write(json.dumps(entry, default=str) + "\n")
        except Exception as e:
            # logging should never crash the server
            print(f"[AUDIT LOG ERROR] {e}")

    @property
    def path(self) -> str:
        return self._path


def _safe_serialize(obj: Any) -> Any:
    """Convert to JSON-safe types."""
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    return obj
