from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import json
import math
import pandas as pd
import yfinance as yf
from tqdm.auto import tqdm


def _sanitize(obj: Any) -> Any:
    """
    Make yfinance 'info' JSON-serializable + stable for parquet storage.
    - Convert NaN/Inf -> None
    - Convert string 'Infinity'/'NaN' variants -> None
    - Convert timestamps/unknown objects -> str
    """
    # Handle numpy/pandas scalars via float conversion where possible
    if obj is None:
        return None

    # Strings like "Infinity"
    if isinstance(obj, str):
        s = obj.strip().lower()
        if s in {"infinity", "+infinity", "-infinity", "inf", "+inf", "-inf", "nan"}:
            return None
        return obj

    # Numbers
    if isinstance(obj, (int, bool)):
        return obj

    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj

    # pandas timestamp / datetime-like
    if hasattr(obj, "isoformat") and callable(getattr(obj, "isoformat")):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)

    # Dict / list recursion
    if isinstance(obj, dict):
        return {str(k): _sanitize(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]

    # Fallback for anything else (e.g., weird objects)
    return str(obj)


@dataclass
class YFClient:
    cache_enabled: bool
    cache_path: Path

    def __post_init__(self) -> None:
        self._cache: Dict[str, Dict[str, Any]] = {}

        if not self.cache_enabled:
            return

        if self.cache_path.exists():
            try:
                df = pd.read_parquet(self.cache_path)

                # New format: info_json is a JSON string
                if "info_json" in df.columns:
                    for _, row in df.iterrows():
                        t = str(row["ticker"])
                        raw = row["info_json"]
                        if isinstance(raw, str):
                            try:
                                self._cache[t] = json.loads(raw)
                            except Exception:
                                self._cache[t] = {}
                        elif isinstance(raw, dict):
                            # If you somehow have dicts in old cache
                            self._cache[t] = raw
                        else:
                            self._cache[t] = {}
            except Exception:
                # If cache is corrupt/incompatible, ignore it
                self._cache = {}

    def save_cache(self) -> None:
        if not self.cache_enabled:
            return

        self.cache_path.parent.mkdir(parents=True, exist_ok=True)

        rows = []
        for t, info in self._cache.items():
            safe_info = _sanitize(info)
            rows.append(
                {
                    "ticker": t,
                    "info_json": json.dumps(safe_info, ensure_ascii=False),
                }
            )

        df = pd.DataFrame(rows)
        df.to_parquet(self.cache_path, index=False)

    def get_info(self, ticker: str) -> Dict[str, Any]:
        if ticker in self._cache:
            return self._cache[ticker]

        try:
            info = yf.Ticker(ticker).info or {}
        except Exception:
            info = {}

        # Store even if empty to avoid repeated failures
        self._cache[ticker] = info
        return info

    def get_infos_bulk(self, tickers: list[str]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for t in tqdm(tickers, desc="Fetching yfinance info"):
            out[t] = self.get_info(t)
        return out
