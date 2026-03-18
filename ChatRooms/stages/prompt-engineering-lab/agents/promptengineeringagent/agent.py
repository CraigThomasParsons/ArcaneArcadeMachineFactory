"""Custom stage agent hook for TheFactoryHopper."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List


def transform(job: Dict[str, Any], package_dir: Path, out_dir: Path) -> List[str]:
    """Return additional relative artifact refs after stage processing."""
    # Add stage-specific custom behavior here when needed.
    return []
