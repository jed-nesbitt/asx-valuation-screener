# ASX Valuation Screener (Python)

A modular Python screener for **ASX-listed companies** that pulls valuation/size/profitability metrics from **yfinance**, runs **multiple screening strategies**, and exports:
- `tickers.csv` (tickers only)
- `tickers_with_strategy.csv` (**wide + deduped**, one row per ticker)
- `tickers_with_strategy_long.csv` (long/stacked)
- per-strategy CSVs like `low_pe_absolute_overall.csv`
- `industry_average_pe.csv` (industry pivot table across metrics)

> ⚠️ Not financial advice. This is a data tool. Always validate outputs with primary sources.

---

## Features

- Multi-file, maintainable structure
- Progress bar for yfinance fetching
- Multiple strategies (each runs **Top N overall** + **Top M per industry**)
- Industry “pivot” output with averages/medians and coverage counts
- Optional caching to speed up reruns

---

## Project Structure

## Project Structure

```text
asx-valuation-screener/
  main.py
  config.py
  io_asx.py
  yf_client.py
  metrics.py
  strategies.py
  industry_pivot.py
  outputs.py
  requirements.txt
  data/
    ASXListedCompanies.csv
  outputs/
    (generated files)
