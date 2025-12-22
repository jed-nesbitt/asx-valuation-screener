from __future__ import annotations

import pandas as pd


def build_industry_pivot(metrics_df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a pivot-style table:
    one row per industry, many columns (avg/median + coverage counts).
    """

    # Only keep columns we might aggregate
    cols = [
        "industry",
        "pe",
        "market_cap",
        "eps",
        "price_to_book",
        "dividend_yield",
    ]
    df = metrics_df[[c for c in cols if c in metrics_df.columns]].copy()

    def n_nonnull(s: pd.Series) -> int:
        return int(s.notna().sum())

    agg = {"industry": "size"}  # placeholder, we'll overwrite with proper agg below

    # Build aggregation dict dynamically for existing numeric cols
    agg_dict = {}
    for col in df.columns:
        if col == "industry":
            continue
        agg_dict[col] = ["mean", "median", n_nonnull]

    pivot = (
        df.groupby("industry", dropna=False)
        .agg(agg_dict)
    )

    # Flatten MultiIndex columns: ("pe","mean") -> "pe_mean"
    pivot.columns = [f"{c0}_{c1}" for (c0, c1) in pivot.columns]
    pivot = pivot.reset_index()

    # Optional: nicer names
    rename_map = {
        "pe_mean": "avg_pe",
        "pe_median": "median_pe",
        "pe_n_nonnull": "n_pe",
        "market_cap_mean": "avg_market_cap",
        "market_cap_median": "median_market_cap",
        "market_cap_n_nonnull": "n_market_cap",
        "eps_mean": "avg_eps",
        "eps_median": "median_eps",
        "eps_n_nonnull": "n_eps",
        "price_to_book_mean": "avg_price_to_book",
        "price_to_book_median": "median_price_to_book",
        "price_to_book_n_nonnull": "n_price_to_book",
        "dividend_yield_mean": "avg_dividend_yield",
        "dividend_yield_median": "median_dividend_yield",
        "dividend_yield_n_nonnull": "n_dividend_yield",
    }
    pivot = pivot.rename(columns={k: v for k, v in rename_map.items() if k in pivot.columns})

    # Sort industries alphabetically (or sort by avg_pe etc if you want)
    pivot = pivot.sort_values("industry")

    return pivot
