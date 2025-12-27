from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

SelectionMode = Literal["per_industry", "overall"]


@dataclass(frozen=True)
class StrategyResult:
    selections: pd.DataFrame
    ascending: bool  # True => lower metric_value is better; False => higher is better
    industry_pivot: pd.DataFrame | None = None


def _select(df: pd.DataFrame, mode: SelectionMode, n: int, sort_col: str, ascending: bool) -> pd.DataFrame:
    if mode == "overall":
        return df.sort_values(sort_col, ascending=ascending).head(n).copy()

    # per_industry
    df_sorted = df.sort_values(["industry", sort_col], ascending=[True, ascending]).copy()
    return df_sorted.groupby("industry", as_index=False, group_keys=False).head(n).copy()


def low_pe_absolute(df: pd.DataFrame, mode: SelectionMode, n: int) -> StrategyResult:
    base = df.dropna(subset=["pe"]).copy()
    sel = _select(base, mode=mode, n=n, sort_col="pe", ascending=True)
    sel["metric_name"] = "pe"
    sel["metric_value"] = sel["pe"]
    return StrategyResult(selections=sel, ascending=True)


def high_market_cap(df: pd.DataFrame, mode: SelectionMode, n: int) -> StrategyResult:
    base = df.dropna(subset=["market_cap"]).copy()
    sel = _select(base, mode=mode, n=n, sort_col="market_cap", ascending=False)
    sel["metric_name"] = "market_cap"
    sel["metric_value"] = sel["market_cap"]
    return StrategyResult(selections=sel, ascending=False)


def high_eps(df: pd.DataFrame, mode: SelectionMode, n: int) -> StrategyResult:
    base = df.dropna(subset=["eps"]).copy()
    sel = _select(base, mode=mode, n=n, sort_col="eps", ascending=False)
    sel["metric_name"] = "eps"
    sel["metric_value"] = sel["eps"]
    return StrategyResult(selections=sel, ascending=False)


def high_dividend_yield(df: pd.DataFrame, mode: SelectionMode, n: int) -> StrategyResult:
    base = df.dropna(subset=["dividend_yield"]).copy()

    # Optional cleaning: ignore non-positive yields
    base.loc[base["dividend_yield"] <= 0, "dividend_yield"] = np.nan
    base = base.dropna(subset=["dividend_yield"]).copy()

    sel = _select(base, mode=mode, n=n, sort_col="dividend_yield", ascending=False)
    sel["metric_name"] = "dividend_yield"
    sel["metric_value"] = sel["dividend_yield"]
    return StrategyResult(selections=sel, ascending=False)



def low_pe_relative_industry(df: pd.DataFrame, mode: SelectionMode, n: int) -> StrategyResult:
    base = df.dropna(subset=["pe", "industry"]).copy()

    industry_avg = (
        base.groupby("industry")["pe"]
        .mean()
        .reset_index()
        .rename(columns={"pe": "avg_pe"})
        .sort_values("avg_pe", ascending=True)
    )

    merged = base.merge(industry_avg, on="industry", how="left")
    merged["pe_relative"] = merged["pe"] / merged["avg_pe"]
    merged = merged.replace([np.inf, -np.inf], np.nan).dropna(subset=["pe_relative"])

    sel = _select(merged, mode=mode, n=n, sort_col="pe_relative", ascending=True)
    sel["metric_name"] = "pe_relative"
    sel["metric_value"] = sel["pe_relative"]

    return StrategyResult(selections=sel, ascending=True, industry_pivot=industry_avg)


STRATEGY_FUNCS = {
    "low_pe_absolute": low_pe_absolute,
    "high_market_cap": high_market_cap,
    "high_eps": high_eps,
    "high_dividend_yield": high_dividend_yield,  # add this
    "low_pe_relative_industry": low_pe_relative_industry,
}
