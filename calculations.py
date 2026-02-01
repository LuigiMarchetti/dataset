import json
import pandas as pd
import numpy as np
import yfinance as yf
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
TICKER = "JNJ"
DATA_DIR = Path(".")  # Current directory
OUTPUT_CSV = "jnj_fundamentals_full.csv"
CAGR_YEARS = 5

# -----------------------------
# LOAD JSON FILES
# -----------------------------
def load_statement(path, date_col="date", data_key="data"):
    with open(path, "r") as f:
        data = json.load(f)

    # Handle different JSON structures
    if data_key in data:
        df = pd.DataFrame(data[data_key])
    elif "annualReports" in data:
        df = pd.DataFrame(data["annualReports"])
        date_col = "fiscalDateEnding"  # Use fiscalDateEnding for annualReports
    else:
        raise ValueError(f"Unknown JSON structure in {path}")

    df[date_col] = pd.to_datetime(df[date_col])
    df = df.set_index(date_col).sort_index()
    return df

income = load_statement(DATA_DIR / "jnj_incomestatement.json")
balance = load_statement(DATA_DIR / "jnj_balancesheet.json")
cashflow = load_statement(DATA_DIR / "jnj_cashflow.json")
shares = load_statement(DATA_DIR / "jnj_sharesoutstanding.json")

# numeric conversion
for df in [income, balance, cashflow, shares]:
    df[:] = df.apply(pd.to_numeric, errors="coerce")

# -----------------------------
# MERGE STATEMENTS
# -----------------------------
df = income.join(balance, how="inner", rsuffix="_bal")
df = df.join(cashflow, how="inner", rsuffix="_cf")
df = df.join(shares[["shares_outstanding_diluted"]], how="inner")

# -----------------------------
# DOWNLOAD HISTORICAL PRICES
# -----------------------------
prices_df = yf.download(TICKER, start=df.index.min(), progress=False)
# Handle both single ticker (Series/DataFrame) and multi-ticker cases
if isinstance(prices_df.columns, pd.MultiIndex):
    # For multi-ticker downloads, columns are MultiIndex with ('Price', 'Ticker') structure
    # Try Adj Close first, fallback to Close
    if ('Adj Close', TICKER) in prices_df.columns:
        prices = prices_df[("Adj Close", TICKER)]
    else:
        prices = prices_df[("Close", TICKER)]
else:
    # For single ticker, columns are regular Index
    if "Adj Close" in prices_df.columns:
        prices = prices_df["Adj Close"]
    else:
        prices = prices_df["Close"]
prices = prices.dropna()

# align price to fiscal date (revealed info)
df["price"] = [
    prices[prices.index >= d].iloc[0] if len(prices[prices.index >= d]) else np.nan
    for d in df.index
]

# -----------------------------
# CORE METRICS
# -----------------------------
df["market_cap"] = df["price"] * df["shares_outstanding_diluted"]

df["eps"] = df["net_income"] / df["shares_outstanding_diluted"]
df["pe"] = df["price"] / df["eps"]

df["book_equity"] = df["total_assets"] - df["total_liabilities"]
df["book_to_market"] = df["book_equity"] / df["market_cap"]

df["roe"] = df["net_income"] / df["book_equity"]
df["roa"] = df["net_income"] / df["total_assets"]

# ROIC
df["nopat"] = df["operating_income"] * (1 - df["income_tax_expense"] / df["income_before_tax"])
df["invested_capital"] = (
        df["total_debt"] +
        df["total_equity"] -
        df["cash_and_cash_equivalents"]
)
df["roic"] = df["nopat"] / df["invested_capital"]

# Margins
df["operating_margin"] = df["operating_income"] / df["revenue"]

# Cash / dividends
df["capex"] = df["capital_expenditure"].abs()
df["dividend_yield"] = df["dividends_paid"].abs() / df["market_cap"]
df["payout"] = df["dividends_paid"].abs() / df["net_income"]

# Debt
df["debt"] = df["total_debt"]

# -----------------------------
# CAGR (rolling)
# -----------------------------
def cagr(series, years):
    return (series / series.shift(years)) ** (1 / years) - 1

df["revenue_cagr"] = cagr(df["revenue"], CAGR_YEARS)
df["eps_cagr"] = cagr(df["eps"], CAGR_YEARS)

# -----------------------------
# FINAL CLEANUP
# -----------------------------
final_cols = [
    "price", "market_cap",
    "roe", "roa", "roic",
    "eps", "pe", "book_to_market",
    "dividend_yield", "payout",
    "operating_margin",
    "capex", "revenue", "net_income", "debt",
    "revenue_cagr", "eps_cagr"
]

final_df = df[final_cols].dropna(how="all")
final_df.to_csv(OUTPUT_CSV)

print(f"Saved {OUTPUT_CSV}")
