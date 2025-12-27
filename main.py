from __future__ import annotations

import json
import platform
import socket
import getpass
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd

from src.config import Config
from src.io_asx import load_asx_list
from src.yf_client import YFClient
from src.metrics import build_metrics
from src.strategies import STRATEGY_FUNCS
from src.outputs import (
    ensure_outdir,
    save_tickers_only,
    save_tickers_with_strategy_long,
    save_tickers_with_strategy_wide,
    save_strategy_mode_csv,
)
from src.industry_pivot import build_industry_pivot


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


def _make_run_dir(runs_root: Path) -> tuple[Path, str]:
    """
    Creates outputs/runs/<timestamp>_<8charid>/ and returns (run_dir, run_id).
    """
    ts = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    run_id = f"{ts}_{uuid4().hex[:8]}"
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    return run_dir, run_id


def _write_run_metadata(run_dir: Path, metadata: dict) -> Path:
    path = run_dir / "run_metadata.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, sort_keys=True, default=str)
    return path


def _safe_cfg_dict(cfg: Config) -> dict:
    # best-effort config serialization
    d = dict(vars(cfg))
    # strategies often contain objects (dataclasses); serialize them safely
    if "strategies" in d and isinstance(d["strategies"], list):
        d["strategies"] = [dict(vars(s)) if hasattr(s, "__dict__") else str(s) for s in d["strategies"]]
    return d


def main() -> None:
    cfg = Config()

    # Base outputs folder stays stable (good for caches, etc.)
    ensure_outdir(cfg.out_dir)

    # Timestamped run folder lives inside outputs/runs/<run_id>/
    runs_root = cfg.out_dir / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)
    run_dir, run_id = _make_run_dir(runs_root)

    t0 = time.time()
    start_utc = datetime.now(timezone.utc).isoformat()
    start_local = datetime.now().astimezone().isoformat()

    metadata: dict = {
        "run_id": run_id,
        "status": "running",
        "start_utc": start_utc,
        "start_local": start_local,
        "base_out_dir": str(cfg.out_dir),
        "run_dir": str(run_dir),
        "environment": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "hostname": socket.gethostname(),
            "user": getpass.getuser(),
        },
        "config": _safe_cfg_dict(cfg),
    }

    # We'll fill these later
    output_files: list[str] = []
    tickers: list[str] = []
    selected_rows = 0

    try:
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
        industry_pivot_path = run_dir / cfg.industry_avg_pe_csv
        industry_pivot.to_csv(industry_pivot_path, index=False)
        output_files.append(str(industry_pivot_path))

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

                    save_strategy_mode_csv(sel, run_dir, spec.name, "overall")
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

                    # add industry average column for this strategy
                    sel = _add_industry_avg_for_strategy(sel, metrics_df)

                    save_strategy_mode_csv(sel, run_dir, spec.name, "per_industry")
                    all_selected.append(sel)

        selected = pd.concat(all_selected, ignore_index=True) if all_selected else pd.DataFrame()
        selected_rows = int(len(selected))

        # 5) Outputs
        tickers_only_path = run_dir / cfg.tickers_only_csv
        save_tickers_only(selected, tickers_only_path)
        output_files.append(str(tickers_only_path))

        long_path = run_dir / "tickers_with_strategy_long.csv"
        save_tickers_with_strategy_long(selected, long_path)
        output_files.append(str(long_path))

        wide_path = run_dir / cfg.tickers_with_strategy_csv
        save_tickers_with_strategy_wide(selected, wide_path)
        output_files.append(str(wide_path))

        metadata["status"] = "success"

        print("\nDone.")
        print(f"- Run folder:           {run_dir}")
        print(f"- Saved industry pivot: {industry_pivot_path}")
        print(f"- Saved tickers only:   {tickers_only_path}")
        print(f"- Saved combined (long): {long_path}")
        print(f"- Saved combined (wide, dedup): {wide_path}")
        print(f"- Saved per-strategy files in: {run_dir}")

    except Exception as e:
        metadata["status"] = "error"
        metadata["error"] = {"type": type(e).__name__, "message": str(e)}
        raise

    finally:
        metadata["end_utc"] = datetime.now(timezone.utc).isoformat()
        metadata["duration_seconds"] = round(time.time() - t0, 3)
        metadata["counts"] = {
            "tickers_loaded": int(len(tickers)),
            "selected_rows": int(selected_rows),
        }
        metadata["output_files"] = output_files

        meta_path = _write_run_metadata(run_dir, metadata)
        print(f"- Saved run metadata:   {meta_path}")


if __name__ == "__main__":
    main()
