import requests
import json
import pandas as pd
import numpy as np
import yfinance as yf
import sys
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
TICKER = "AAPL"  # Set your stock ticker here
API_KEY = "7TIGAMBEH79LXD1B"  # Add your Alpha Vantage API key here
BASE_URL = "https://www.alphavantage.co/query"
CAGR_YEARS = 5

# Validate API key
if not API_KEY or API_KEY == "":
    print("=" * 60)
    print("❌ ERROR: API_KEY is not set!")
    print("=" * 60)
    print("Please add your Alpha Vantage API key to the script.")
    print("Get your free API key at: https://www.alphavantage.co/support/#api-key")
    print("It takes less than 20 seconds to get one.")
    print("=" * 60)
    sys.exit(1)

# Create output directories
RESPONSE_DIR = Path("alphavantage_response")
RESPONSE_DIR.mkdir(exist_ok=True)

CSV_DIR = Path("alphavantage_csv")
CSV_DIR.mkdir(exist_ok=True)

# -----------------------------
# FETCH DATA FROM ALPHA VANTAGE
# -----------------------------
def fetch_alpha_vantage(function_name, ticker):
    """Fetch data from Alpha Vantage API and validate response"""
    params = {
        "function": function_name,
        "symbol": ticker,
        "apikey": API_KEY
    }

    print(f"Fetching {function_name} for {ticker}...")
    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()

    data = r.json()

    # Check for API errors
    if "Error Message" in data:
        print(f"❌ API Error: {data['Error Message']}")
        return None

    if "Note" in data:
        print(f"❌ API Limit: {data['Note']}")
        return None

    if "Information" in data:
        print(f"⚠️  API Info: {data['Information']}")
        return None

    # Check if response has expected data
    if "annualReports" not in data and "quarterlyReports" not in data:
        print(f"❌ No data found in response for {function_name}")
        return None

    # Check if annualReports is empty
    if "annualReports" in data and len(data["annualReports"]) == 0:
        print(f"⚠️  Warning: Empty annualReports for {function_name}")

    return data

# Fetch all 4 data sources
print("=" * 60)
print(f"Fetching data for {TICKER}")
print("=" * 60)

# Track if all fetches were successful
fetch_success = True

# 1. Balance Sheet
balance_data = fetch_alpha_vantage("BALANCE_SHEET", TICKER)
if balance_data is None:
    print("❌ Failed to fetch Balance Sheet. Stopping.")
    fetch_success = False
else:
    balance_path = RESPONSE_DIR / f"{TICKER.lower()}_balancesheet.json"
    with open(balance_path, "w") as f:
        json.dump(balance_data, f, indent=2)
    print(f"✓ Saved {balance_path}")

if not fetch_success:
    sys.exit(1)

# 2. Cash Flow
cashflow_data = fetch_alpha_vantage("CASH_FLOW", TICKER)
if cashflow_data is None:
    print("❌ Failed to fetch Cash Flow. Stopping.")
    fetch_success = False
else:
    cashflow_path = RESPONSE_DIR / f"{TICKER.lower()}_cashflow.json"
    with open(cashflow_path, "w") as f:
        json.dump(cashflow_data, f, indent=2)
    print(f"✓ Saved {cashflow_path}")

if not fetch_success:
    sys.exit(1)

# 3. Income Statement
income_data = fetch_alpha_vantage("INCOME_STATEMENT", TICKER)
if income_data is None:
    print("❌ Failed to fetch Income Statement. Stopping.")
    fetch_success = False
else:
    income_path = RESPONSE_DIR / f"{TICKER.lower()}_incomestatement.json"
    with open(income_path, "w") as f:
        json.dump(income_data, f, indent=2)
    print(f"✓ Saved {income_path}")

if not fetch_success:
    sys.exit(1)

# 4. Shares Outstanding (using earnings endpoint as proxy)
# Note: Alpha Vantage doesn't have a direct shares outstanding endpoint
# We'll extract it from balance sheet data (commonStockSharesOutstanding)
# Create a shares outstanding file in the same format as jnj_sharesoutstanding.json
shares_data = {
    "symbol": TICKER,
    "status": "success",
    "data": []
}

# Extract shares from quarterly balance sheet data if available
if "quarterlyReports" in balance_data:
    for report in balance_data["quarterlyReports"]:
        if "commonStockSharesOutstanding" in report and report["commonStockSharesOutstanding"] != "None":
            shares_data["data"].append({
                "date": report["fiscalDateEnding"],
                "shares_outstanding_diluted": report["commonStockSharesOutstanding"],
                "shares_outstanding_basic": report["commonStockSharesOutstanding"]
            })

if len(shares_data["data"]) == 0:
    print("⚠️  Warning: No shares outstanding data found in balance sheet")

shares_path = RESPONSE_DIR / f"{TICKER.lower()}_sharesoutstanding.json"
with open(shares_path, "w") as f:
    json.dump(shares_data, f, indent=2)
print(f"✓ Saved {shares_path}")

print("\n" + "=" * 60)
print("PROCESSING DATA WITH CALCULATIONS")
print("=" * 60)

# -----------------------------
# LOAD JSON FILES (same logic as calculations.py)
# -----------------------------
def load_statement(path, date_col="date", data_key="data"):
    """Load and parse financial statement from JSON file"""
    with open(path, "r") as f:
        data = json.load(f)

    # Check for API errors in saved files
    if "Error Message" in data:
        print(f"❌ Error in {path}: {data['Error Message']}")
        return None

    if "Note" in data:
        print(f"❌ API Limit in {path}: {data['Note']}")
        return None

    if "Information" in data:
        print(f"⚠️  Info in {path}: {data['Information']}")
        return None

    # Handle different JSON structures
    if data_key in data:
        df = pd.DataFrame(data[data_key])
    elif "annualReports" in data:
        df = pd.DataFrame(data["annualReports"])
        date_col = "fiscalDateEnding"  # Use fiscalDateEnding for annualReports
    else:
        print(f"❌ Unknown JSON structure in {path}")
        return None

    if df.empty:
        print(f"⚠️  Warning: Empty dataframe from {path}")
        return pd.DataFrame()

    df[date_col] = pd.to_datetime(df[date_col])
    df = df.set_index(date_col).sort_index()
    return df

try:
    income = load_statement(income_path)
    if income is None:
        print("❌ Failed to load income statement. Stopping.")
        sys.exit(1)

    balance = load_statement(balance_path)
    if balance is None:
        print("❌ Failed to load balance sheet. Stopping.")
        sys.exit(1)

    cashflow = load_statement(cashflow_path)
    if cashflow is None:
        print("❌ Failed to load cash flow. Stopping.")
        sys.exit(1)

    shares = load_statement(shares_path)
    if shares is None:
        print("❌ Failed to load shares outstanding. Stopping.")
        sys.exit(1)

    # Check if we have any data to work with
    if income.empty:
        print("❌ Income statement is empty. Cannot proceed.")
        sys.exit(1)

    # numeric conversion
    for df in [income, balance, cashflow, shares]:
        if not df.empty:
            df[:] = df.apply(pd.to_numeric, errors="coerce")

    # Convert shares quarterly data to annual by taking year-end values
    if not shares.empty:
        shares_annual = shares.groupby(shares.index.year).last()
        # Reconstruct the index to match fiscal year end dates (Dec 31)
        shares_annual.index = pd.to_datetime([f"{year}-12-31" for year in shares_annual.index])
    else:
        shares_annual = pd.DataFrame()

    # -----------------------------
    # MERGE STATEMENTS
    # -----------------------------
    # Use left join to keep all income statement dates, even if other data is missing
    df = income.join(balance, how="left", rsuffix="_bal")
    df = df.join(cashflow, how="left", rsuffix="_cf")
    if not shares_annual.empty and "shares_outstanding_diluted" in shares_annual.columns:
        df = df.join(shares_annual[["shares_outstanding_diluted"]], how="left")

    # Create column name aliases for easier access (map camelCase to snake_case for readability)
    column_mapping = {
        # Income statement
        'netIncome': 'net_income',
        'totalRevenue': 'revenue',
        'operatingIncome': 'operating_income',
        'incomeBeforeTax': 'income_before_tax',
        'incomeTaxExpense': 'income_tax_expense',

        # Balance sheet
        'totalAssets': 'total_assets',
        'totalLiabilities': 'total_liabilities',
        'totalShareholderEquity': 'total_equity',
        'cashAndCashEquivalentsAtCarryingValue': 'cash_and_cash_equivalents',
        'longTermDebt': 'long_term_debt',
        'shortTermDebt': 'short_term_debt',

        # Cash flow
        'capitalExpenditures': 'capital_expenditure',
        'dividendPayout': 'dividends_paid',
    }

    # Rename columns
    df = df.rename(columns=column_mapping)

    # Calculate total_debt if needed (long_term_debt + short_term_debt)
    if 'long_term_debt' in df.columns and 'short_term_debt' in df.columns:
        df['total_debt'] = df['long_term_debt'].fillna(0) + df['short_term_debt'].fillna(0)
    elif 'long_term_debt' in df.columns:
        df['total_debt'] = df['long_term_debt']
    else:
        df['total_debt'] = 0

    # -----------------------------
    # DOWNLOAD HISTORICAL PRICES
    # -----------------------------
    print(f"Downloading price data for {TICKER}...")
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

    print(f"Total rows before filtering: {len(df)}")
    print(f"Date range: {df.index.min()} to {df.index.max()}")

    # Keep rows that have at least some key metrics (not all NaN)
    final_df = df[final_cols].dropna(how="all")
    final_df = final_df[final_df[["revenue", "net_income"]].notna().any(axis=1)]  # Must have at least revenue or net_income

    print(f"Rows after filtering: {len(final_df)}")

    # Save to CSV with ticker name
    output_csv = CSV_DIR / f"{TICKER.lower()}_fundamentals.csv"
    final_df.to_csv(output_csv)

    print(f"\n✓ Saved {output_csv}")
    print("\n" + "=" * 60)
    print("COMPLETE!")
    print("=" * 60)

except Exception as e:
    print(f"\n❌ Error during processing: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

