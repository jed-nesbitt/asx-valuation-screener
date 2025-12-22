from __future__ import annotations

from typing import Any, Dict, Iterable
import numpy as np
import pandas as pd


def _first_present(info: Dict[str, Any], keys: Iterable[str]) -> Any:
    for k in keys:
        v = info.get(k)
        if v is not None:
            return v
    return None


def _to_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        v = float(x)
        if np.isfinite(v):
            return v
        return None
    except Exception:
        return None


def build_metrics(companies: pd.DataFrame, infos: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for _, r in companies.iterrows():
        t = r["yf_ticker"]
        info = infos.get(t, {}) or {}

        pe = _to_float(_first_present(info, ["trailingPE", "forwardPE"]))
        market_cap = _to_float(_first_present(info, ["marketCap"]))
        eps = _to_float(_first_present(info, ["trailingEps", "forwardEps"]))

        # Optional extras you may want later
        pb = _to_float(_first_present(info, ["priceToBook"]))
        div_yield = _to_float(_first_present(info, ["dividendYield"]))

        rows.append(
            {
                "yf_ticker": t,
                "company_name": r.get("Company name", None),
                "asx_code": r.get("ASX code", None),
                "industry": r.get("GICS industry group", "Unknown"),
                "pe": pe,
                "market_cap": market_cap,
                "eps": eps,
                "price_to_book": pb,
                "dividend_yield": div_yield,
            }
        )

    df = pd.DataFrame(rows)

    # Clean obvious junk
    for col in ["pe", "market_cap", "eps", "price_to_book", "dividend_yield"]:
        df[col] = df[col].replace([np.inf, -np.inf], np.nan)

    # Standard finance conventions: PE <= 0 not meaningful for "cheap"
    df.loc[df["pe"].notna() & (df["pe"] <= 0), "pe"] = np.nan

    return df
