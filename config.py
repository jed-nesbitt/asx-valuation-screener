from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


@dataclass(frozen=True)
class StrategySpec:
    name: str
    top_overall: int | None = None       # N (overall)
    top_per_industry: int | None = None  # M (per industry)


@dataclass(frozen=True)
class Config:
    asx_listed_companies_csv: Path = Path("data/ASXListedCompanies.csv")
    max_tickers: int | None = None

    out_dir: Path = Path("outputs")
    tickers_only_csv: str = "tickers.csv"
    tickers_with_strategy_csv: str = "tickers_with_strategy.csv"
    industry_avg_pe_csv: str = "industry_average_pe.csv"

    cache_enabled: bool = False
    cache_path: Path = Path("outputs/yf_info_cache.parquet")

    strategies: Sequence[StrategySpec] = (
        StrategySpec(name="low_pe_relative_industry", top_overall=50, top_per_industry=2),
        StrategySpec(name="low_pe_absolute",          top_overall=50, top_per_industry=2),
        StrategySpec(name="high_market_cap",          top_overall=50, top_per_industry=2),
        StrategySpec(name="high_eps",                 top_overall=50, top_per_industry=2),
        StrategySpec(name="high_dividend_yield",      top_overall=50, top_per_industry=2),
    )
