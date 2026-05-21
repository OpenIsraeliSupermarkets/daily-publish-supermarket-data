"""Helpers for values that BSON / MongoDB can represent."""

_MONGO_INT_MAX = 2**63 - 1
_MONGO_INT_MIN = -(2**63)

try:
    import numpy as np

    _NUMPY_INTEGER_TYPES = (np.integer,)
except ImportError:  # pragma: no cover - numpy is a transitive dep (pandas)
    _NUMPY_INTEGER_TYPES = ()


def sanitize_for_mongo(obj):
    """Recursively convert integers that exceed MongoDB's 8-byte limit to strings.

    Also normalizes numpy integer scalars so out-of-range values are detected
    (e.g. unsigned values above 2**63 - 1).
    """
    if isinstance(obj, dict):
        return {k: sanitize_for_mongo(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_mongo(v) for v in obj]
    if isinstance(obj, _NUMPY_INTEGER_TYPES):
        obj = int(obj)
    if isinstance(obj, int) and not isinstance(obj, bool):
        if obj > _MONGO_INT_MAX or obj < _MONGO_INT_MIN:
            return str(obj)
    return obj
