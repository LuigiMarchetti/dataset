import yfinance as yf
import pandas as pd

prices_df = yf.download("JNJ", start="2020-01-01", progress=False)
print("Type:", type(prices_df))
print("Columns:", prices_df.columns)
print("Column type:", type(prices_df.columns))
print("Is MultiIndex:", isinstance(prices_df.columns, pd.MultiIndex))

if isinstance(prices_df.columns, pd.MultiIndex):
    print("\nMultiIndex levels:")
    for i, level in enumerate(prices_df.columns.levels):
        print(f"  Level {i}: {level.tolist()}")
    print("\nFirst few columns:")
    for col in prices_df.columns[:5]:
        print(f"  {col}")

print("\nFirst few rows:")
print(prices_df.head())

# Try to access Adj Close
try:
    if isinstance(prices_df.columns, pd.MultiIndex):
        print("\nTrying to access ('Adj Close', 'JNJ'):")
        print(prices_df[("Adj Close", "JNJ")].head())
    else:
        print("\nTrying to access 'Adj Close':")
        print(prices_df["Adj Close"].head())
except Exception as e:
    print(f"\nError accessing: {e}")
    print("\nTrying alternative access...")
    print(prices_df.iloc[:, -1].head())  # Last column is usually Adj Close


