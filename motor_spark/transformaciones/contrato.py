from __future__ import annotations

from collections.abc import Callable
from typing import Any

ManejadorTransformacion = Callable[[Any, Any, int], Any]
