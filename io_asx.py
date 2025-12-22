from __future__ import annotations

from pathlib import Path
import pandas as pd


def _find_header_line(lines: list[str]) -> int:
    for i, line in enumerate(lines):
        if line.startswith("Company name"):
            return i
    raise ValueError("Could not find header line starting with 'Company name'")


def to_yf_ticker(asx_code: str) -> str:
    code = str(asx_code).strip()
    if "." in code:
        return code
    return code + ".AX"


def load_asx_list(csv_path: Path, max_tickers: int | None = None) -> pd.DataFrame:
    lines = csv_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    header_idx = _find_header_line(lines)

    df = pd.read_csv(csv_path, skiprows=header_idx)

    if max_tickers is not None:
        df = df.head(max_tickers).copy()

    # Standardize key columns (ASX file sometimes varies slightly)
    if "ASX code" not in df.columns:
        raise ValueError("Expected column 'ASX code' not found in ASXListedCompanies.csv")

    df["yf_ticker"] = df["ASX code"].apply(to_yf_ticker)

    # Keep only what we need
    keep = [c for c in ["Company name", "ASX code", "GICS industry group", "yf_ticker"] if c in df.columns]
    df = df[keep].copy()

    # Fill missing industry group if any
    if "GICS industry group" not in df.columns:
        df["GICS industry group"] = "Unknown"

    return df
