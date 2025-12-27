from __future__ import annotations

from pathlib import Path
import pandas as pd


def ensure_outdir(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)


# ---------- LONG (stacked) EXPORT ----------
def _format_selection_long(selected: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "yf_ticker",
        "industry",
        "strategy",
        "mode",
        "rank",
        "metric_name",
        "metric_value",
        "industry_avg",
        "company_name",
        "asx_code",
    ]
    keep = [c for c in cols if c in selected.columns]
    out = selected[keep].copy()
    if "yf_ticker" in out.columns:
        out = out.rename(columns={"yf_ticker": "ticker"})
    return out


def save_tickers_only(selected: pd.DataFrame, out_path: Path) -> None:
    if "yf_ticker" not in selected.columns:
        pd.DataFrame({"ticker": []}).to_csv(out_path, index=False)
        return

    tickers = (
        selected[["yf_ticker"]]
        .dropna()
        .drop_duplicates()
        .rename(columns={"yf_ticker": "ticker"})
        .sort_values("ticker")
    )
    tickers.to_csv(out_path, index=False)


def save_tickers_with_strategy_long(selected: pd.DataFrame, out_path: Path) -> None:
    out = _format_selection_long(selected)
    out.to_csv(out_path, index=False)


# ---------- WIDE (DEDUP) EXPORT ----------
def save_tickers_with_strategy_wide(selected: pd.DataFrame, out_path: Path) -> None:
    """
    One row per ticker (no repeats). Columns like:
      low_pe_absolute_overall_rank
      low_pe_absolute_overall_value
      ...
    """
    df = selected.copy()

    # Required columns safety
    for c in ["yf_ticker", "industry", "strategy", "mode", "rank", "metric_value"]:
        if c not in df.columns:
            df[c] = None

    # Ensure ticker is string-ish and drop null tickers
    df["yf_ticker"] = df["yf_ticker"].astype(str)
    df = df[df["yf_ticker"].notna() & (df["yf_ticker"].str.len() > 0)].copy()

    # Pivot ranks -> one row per ticker
    rank_wide = df.pivot_table(
        index="yf_ticker",
        columns=["strategy", "mode"],
        values="rank",
        aggfunc="min",
    )

    # Pivot metric values -> one row per ticker
    val_wide = df.pivot_table(
        index="yf_ticker",
        columns=["strategy", "mode"],
        values="metric_value",
        aggfunc="first",
    )

    # Pick a single industry per ticker (first non-null)
    industry_map = (
        df[["yf_ticker", "industry"]]
        .dropna(subset=["industry"])
        .drop_duplicates(subset=["yf_ticker"])
        .set_index("yf_ticker")["industry"]
    )

    # Flatten columns
    def _flat(cols, suffix: str) -> list[str]:
        return [f"{a}_{b}_{suffix}" for (a, b) in cols]

    rank_wide.columns = _flat(rank_wide.columns, "rank")
    val_wide.columns = _flat(val_wide.columns, "value")

    wide = pd.concat([rank_wide, val_wide], axis=1)

    # Attach industry (optional but useful)
    wide.insert(0, "industry", industry_map.reindex(wide.index))

    wide = wide.reset_index().rename(columns={"yf_ticker": "ticker"})

    # Summary column: where it was selected
    rank_cols = [c for c in wide.columns if c.endswith("_rank")]
    wide["selected_in"] = wide[rank_cols].notna().apply(
        lambda r: ", ".join([col.replace("_rank", "") for col, ok in r.items() if ok]),
        axis=1,
    )

    wide.to_csv(out_path, index=False)


def save_strategy_mode_csv(selected: pd.DataFrame, out_dir: Path, strategy: str, mode: str) -> Path:
    """
    Saves e.g. outputs/low_pe_absolute_per_industry.csv
    """
    fname = f"{strategy}_{mode}.csv"
    path = out_dir / fname
    out = _format_selection_long(selected)
    out.to_csv(path, index=False)
    return path
