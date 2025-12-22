from __future__ import annotations

import pandas as pd

from config import Config
from io_asx import load_asx_list
from yf_client import YFClient
from metrics import build_metrics
from strategies import STRATEGY_FUNCS
from outputs import (
    ensure_outdir,
    save_tickers_only,
    save_tickers_with_strategy_long,
    save_tickers_with_strategy_wide,
    save_strategy_mode_csv,
)
from industry_pivot import build_industry_pivot


def _rank_results(sel: pd.DataFrame, mode: str, ascending: bool) -> pd.DataFrame:
    sel = sel.copy()

    if mode == "per_industry":
        if "industry" in sel.columns and "metric_value" in sel.columns:
            sel = sel.sort_values(["industry", "metric_value"], ascending=[True, ascending])
        sel["rank"] = sel.groupby("industry").cumcount() + 1
    else:
        if "metric_value" in sel.columns:
            sel = sel.sort_values(["metric_value"], ascending=ascending)
        sel["rank"] = range(1, len(sel) + 1)

    return sel


def _add_industry_avg_for_strategy(sel: pd.DataFrame, metrics_df: pd.DataFrame) -> pd.DataFrame:
    sel = sel.copy()
    if sel.empty:
        sel["industry_avg"] = None
        return sel

    metric_name = None
    if "metric_name" in sel.columns and sel["metric_name"].notna().any():
        metric_name = str(sel["metric_name"].dropna().iloc[0])

    # pe_relative: industry average is avg PE baseline
    if metric_name == "pe_relative":
        avg = (
            metrics_df.dropna(subset=["pe"])
            .groupby("industry")["pe"]
            .mean()
            .rename("industry_avg")
            .reset_index()
        )
        return sel.merge(avg, on="industry", how="left")

    # normal metrics (including dividend_yield)
    if metric_name and metric_name in metrics_df.columns:
        avg = (
            metrics_df.dropna(subset=[metric_name])
            .groupby("industry")[metric_name]
            .mean()
            .rename("industry_avg")
            .reset_index()
        )
        return sel.merge(avg, on="industry", how="left")

    sel["industry_avg"] = None
    return sel




def main() -> None:
    cfg = Config()
    ensure_outdir(cfg.out_dir)

    # 1) Load ASX list
    companies = load_asx_list(cfg.asx_listed_companies_csv, max_tickers=cfg.max_tickers)
    tickers = companies["yf_ticker"].dropna().astype(str).tolist()

    # 2) Fetch yfinance infos (cached)
    client = YFClient(cache_enabled=cfg.cache_enabled, cache_path=cfg.cache_path)
    infos = client.get_infos_bulk(tickers)
    client.save_cache()

    # 3) Build metrics table
    metrics_df = build_metrics(companies, infos)

    # 3.5) Save industry pivot (multi-metric)
    industry_pivot = build_industry_pivot(metrics_df)
    industry_pivot.to_csv(cfg.out_dir / cfg.industry_avg_pe_csv, index=False)

    # 4) Run strategies: overall top N AND per-industry top M
    all_selected: list[pd.DataFrame] = []

    for spec in cfg.strategies:
        if spec.name not in STRATEGY_FUNCS:
            raise ValueError(f"Unknown strategy: {spec.name}")

        fn = STRATEGY_FUNCS[spec.name]

        # ---- A) Overall top N ----
        if getattr(spec, "top_overall", None):
            n = int(spec.top_overall)
            if n > 0:
                res = fn(metrics_df, mode="overall", n=n)
                sel = res.selections.copy()
                sel["strategy"] = spec.name
                sel["mode"] = "overall"
                sel = _rank_results(sel, mode="overall", ascending=res.ascending)

                # (No industry_avg needed for overall; keep column for consistency)
                sel["industry_avg"] = None

                save_strategy_mode_csv(sel, cfg.out_dir, spec.name, "overall")
                all_selected.append(sel)

        # ---- B) Top M per industry ----
        if getattr(spec, "top_per_industry", None):
            m = int(spec.top_per_industry)
            if m > 0:
                res = fn(metrics_df, mode="per_industry", n=m)
                sel = res.selections.copy()
                sel["strategy"] = spec.name
                sel["mode"] = "per_industry"
                sel = _rank_results(sel, mode="per_industry", ascending=res.ascending)

                # NEW: add industry average column for this strategy
                sel = _add_industry_avg_for_strategy(sel, metrics_df)

                save_strategy_mode_csv(sel, cfg.out_dir, spec.name, "per_industry")
                all_selected.append(sel)

    selected = pd.concat(all_selected, ignore_index=True) if all_selected else pd.DataFrame()

    # 5) Outputs
    save_tickers_only(selected, cfg.out_dir / cfg.tickers_only_csv)

    # Long combined (your current one)
    long_path = cfg.out_dir / "tickers_with_strategy_long.csv"
    save_tickers_with_strategy_long(selected, long_path)

    # Wide combined (NO repeated tickers) -> keep the original name
    wide_path = cfg.out_dir / cfg.tickers_with_strategy_csv
    save_tickers_with_strategy_wide(selected, wide_path)

    print("\nDone.")
    print(f"- Saved industry pivot: {cfg.out_dir / cfg.industry_avg_pe_csv}")
    print(f"- Saved tickers only:   {cfg.out_dir / cfg.tickers_only_csv}")
    print(f"- Saved combined (long): {long_path}")
    print(f"- Saved combined (wide, dedup): {wide_path}")
    print(f"- Saved per-strategy files in: {cfg.out_dir}")


if __name__ == "__main__":
    main()
