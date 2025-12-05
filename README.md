# ASX Relative P/E Screener

Python tool that analyzes ASX stocks, builds industry P/E benchmarks, and selects the cheapest companies relative to their sector.

---

## Overview

This project is a simple ASX stock screener that:

1. Loads an official ASX listed companies file  
2. Converts ASX codes into Yahoo Finance tickers  
3. Fetches trailing/forward P/E ratios using `yfinance`  
4. Cleans and filters out invalid P/E values  
5. Calculates **average P/E by GICS industry group**  
6. Finds the **N cheapest companies per industry** based on *relative P/E*  
7. Saves:
   - A pivot table of industry average P/E  
   - A CSV of the selected cheapest tickers  

It is designed as a lightweight, education-focused valuation tool and idea generator.

---

## Features

- Automatically detects the header row in the ASX CSV
- Converts ASX codes (e.g. `BHP`) into Yahoo Finance tickers (e.g. `BHP.AX`)
- Uses trailing P/E and falls back to forward P/E where needed
- Cleans P/E data:
  - Removes `NaN`, `inf`, `-inf`
  - Drops P/E values ≤ 0
- Computes **industry average P/E** by GICS industry group
- Computes **relative P/E**:  

  \[
  \text{relative P/E} = \frac{\text{company P/E}}{\text{industry average P/E}}
  \]

- Selects the **N cheapest companies per industry** (lowest relative P/E)
- Outputs:
  - `industry_average_pe_full_dataset.csv`
  - `tickers.csv` (list of tickers for further analysis/backtesting)

---

## Requirements

- Python 3.8+
- Packages:
  - `numpy`
  - `pandas`
  - `yfinance`
  - `tqdm`

Install dependencies:

```bash
pip install numpy pandas yfinance tqdm
