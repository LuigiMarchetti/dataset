import yfinance as yf
import pandas as pd

TICKER = "XOM"
ticker = yf.Ticker(TICKER)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

def summarize(name, obj):
    print(f"\n{'='*80}")
    print(name)
    print(f"{'='*80}")

    if obj is None:
        print("None")
        return

    if isinstance(obj, (pd.Series, pd.DataFrame)):
        if obj.empty:
            print("EMPTY")
            return
        print(f"Type: {type(obj)}")
        print(f"Shape: {obj.shape}")
        try:
            print(f"Date range: {obj.index.min()} → {obj.index.max()}")
        except:
            pass
        print("Columns / Index:")
        print(obj.columns if isinstance(obj, pd.DataFrame) else obj.index)
    else:
        print(type(obj))
        print(obj)


# 1. PRICE HISTORY (LONG)
summarize(
    "PRICE HISTORY – DAILY (MAX)",
    ticker.history(period="max", interval="1d")
)

summarize(
    "PRICE HISTORY – MONTHLY (MAX)",
    ticker.history(period="max", interval="1mo")
)

# 2. DIVIDENDS & SPLITS (LONG)
summarize(
    "DIVIDENDS",
    ticker.dividends
)

summarize(
    "SPLITS",
    ticker.splits
)

# 3. FINANCIAL STATEMENTS – ANNUAL
summarize(
    "INCOME STATEMENT – ANNUAL",
    ticker.financials
)

summarize(
    "BALANCE SHEET – ANNUAL",
    ticker.balance_sheet
)

summarize(
    "CASH FLOW – ANNUAL",
    ticker.cashflow
)

# 4. FINANCIAL STATEMENTS – QUARTERLY
summarize(
    "INCOME STATEMENT – QUARTERLY",
    ticker.quarterly_financials
)

summarize(
    "BALANCE SHEET – QUARTERLY",
    ticker.quarterly_balance_sheet
)

summarize(
    "CASH FLOW – QUARTERLY",
    ticker.quarterly_cashflow
)

# 5. EARNINGS / EPS
summarize(
    "INCOME STATEMENT – ANNUAL (AUTHORITATIVE)",
    ticker.income_stmt
)

summarize(
    "EARNINGS HISTORY (EPS)",
    ticker.earnings_history
)

# 6. ACTIONS
summarize(
    "ACTIONS (DIVIDENDS + SPLITS)",
    ticker.actions
)

# 7. HOLDERS
summarize(
    "INSTITUTIONAL HOLDERS",
    ticker.institutional_holders
)

summarize(
    "MAJOR HOLDERS",
    ticker.major_holders
)

# 8. INFO / RATIOS (SNAPSHOT)
summarize(
    "INFO (KEY STATS / RATIOS – SNAPSHOT)",
    ticker.info
)

print(f"\n{'='*80}")
print("END OF PROBE")
print(f"{'='*80}")
