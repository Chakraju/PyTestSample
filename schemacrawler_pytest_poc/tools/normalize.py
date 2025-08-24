import re
from typing import Any, Set

def _norm_sql(s: str) -> str:
    if not isinstance(s, str):
        return s
    return " ".join(s.split())

def canonicalize(obj: Any,
                 ignore_keys: Set[str] = None,
                 normalize_sql_keys: Set[str] = None) -> Any:
    ignore_keys = ignore_keys or set()
    normalize_sql_keys = normalize_sql_keys or set()

    if isinstance(obj, dict):
        new_items = {}
        for k, v in obj.items():
            if k in ignore_keys:
                continue
            v_can = canonicalize(v, ignore_keys, normalize_sql_keys)
            if k in normalize_sql_keys and isinstance(v_can, str):
                v_can = _norm_sql(v_can)
            new_items[k] = v_can
        return {k: new_items[k] for k in sorted(new_items.keys())}

    if isinstance(obj, list):
        items = [canonicalize(x, ignore_keys, normalize_sql_keys) for x in obj]
        try:
            return sorted(items, key=lambda x: str(x))
        except Exception:
            return items

    return obj
