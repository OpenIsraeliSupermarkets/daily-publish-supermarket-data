"""Normalize values for MongoDB/BSON and JSON serialization.

Pandas and NumPy types from CSV rows are not BSON-encodable; this module
recursively converts them to plain Python values.
"""

from __future__ import annotations

import math
from decimal import Decimal
from typing import Any

_MONGO_INT_MAX = 2**63 - 1
_MONGO_INT_MIN = -(2**63)


def sanitize_for_mongo(obj: Any) -> Any:
    """Recursively convert objects to BSON-safe Python types."""
    if isinstance(obj, dict):
        return {k: sanitize_for_mongo(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_mongo(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(sanitize_for_mongo(v) for v in obj)
    if obj is None or isinstance(obj, (bool, bytes, str)):
        return obj

    try:
        import numpy as np

        if isinstance(obj, np.ndarray):
            return sanitize_for_mongo(obj.tolist())
        if isinstance(obj, np.generic):
            return sanitize_for_mongo(obj.item())
    except ImportError:
        pass

    try:
        import pandas as pd

        if obj is pd.NA:
            return None
        if isinstance(obj, pd.Timestamp):
            if pd.isna(obj):
                return None
            return obj
    except ImportError:
        pass

    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None

    if isinstance(obj, Decimal):
        return sanitize_for_mongo(float(obj))

    if isinstance(obj, int) and not isinstance(obj, bool):
        if obj > _MONGO_INT_MAX or obj < _MONGO_INT_MIN:
            return str(obj)

    return obj
