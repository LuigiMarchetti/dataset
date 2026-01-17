import yfinance as yf
import pandas as pd

# ===== CONFIG =====
TICKER = "XOM"
YEARS = 30

def extract_scalar(val):
    """Extrai valor escalar de Series/DataFrame ou retorna o valor"""
    if val is None:
        return None
    if isinstance(val, (pd.Series, pd.DataFrame)):
        try:
            scalar = val.iloc[0] if len(val) > 0 else None
        except Exception:
            return None
        return None if pd.isna(scalar) else scalar
    return None if pd.isna(val) else val

def safe_div(num, den):
    """Divisão segura com extração de valores escalares"""
    num = extract_scalar(num)
    den = extract_scalar(den)
    if num is None or den is None or den == 0:
        return None
    return num / den

# ===== GET DATA =====
ticker = yf.Ticker(TICKER)

# Financials
bs = ticker.balance_sheet
isf = ticker.financials
cf = ticker.cashflow

# Transpose para ter datas como index
bs_annual = bs.T.sort_index()
isf_annual = isf.T.sort_index()
cf_annual = cf.T.sort_index()

# Pegar todas as datas disponíveis (usa timestamps que vem do yfinance)
all_dates = sorted(set(list(isf_annual.index) + list(bs_annual.index) + list(cf_annual.index)))

# ===== CALCULATIONS =====
rows = []

for date in all_dates:
    # Extrair dados para esta data
    inc = isf_annual.loc[date] if date in isf_annual.index else pd.Series()
    bal = bs_annual.loc[date] if date in bs_annual.index else pd.Series()
    csh = cf_annual.loc[date] if date in cf_annual.index else pd.Series()

    # Converter data para ano
    year = pd.to_datetime(date).year

    # Extrair valores brutos
    revenue = extract_scalar(inc.get('Total Revenue'))
    net_income = extract_scalar(inc.get('Net Income'))
    total_assets = extract_scalar(bal.get('Total Assets'))
    total_equity = extract_scalar(bal.get('Total Stockholder Equity'))

    # Dívida total
    short_debt = extract_scalar(bal.get('Short Long Term Debt')) or extract_scalar(bal.get('Current Debt')) or 0
    long_debt = extract_scalar(bal.get('Long Term Debt')) or 0
    total_debt = short_debt + long_debt if (short_debt or long_debt) else None

    # CAPEX
    capex = extract_scalar(csh.get('Capital Expenditures'))

    # Interest expense para ROIC
    interest_expense = extract_scalar(inc.get('Interest Expense')) or 0

    # Cálculo de NOPAT (Net Operating Profit After Tax) para ROIC
    ebit = extract_scalar(inc.get('EBIT'))
    tax_rate = 0.21  # taxa corporativa aproximada nos EUA
    if ebit is not None:
        nopat = ebit * (1 - tax_rate)
    else:
        nopat = None

    # Invested Capital = Total Debt + Total Equity
    invested_capital = None
    if total_debt is not None and total_equity is not None:
        invested_capital = total_debt + total_equity
    elif total_equity is not None:
        invested_capital = total_equity

    # Ratios básicos
    roe = safe_div(net_income, total_equity)
    roa = safe_div(net_income, total_assets)
    roic = safe_div(nopat, invested_capital)

    # Operating margin
    op_income = extract_scalar(inc.get('Operating Income'))
    operating_margin = safe_div(op_income, revenue)

    # EPS e P/E - usar info atual (não histórico perfeito)
    eps = None
    pe = None
    try:
        # Tentar obter EPS básico do income statement
        basic_eps = extract_scalar(inc.get('Basic EPS'))
        if basic_eps:
            eps = basic_eps
    except:
        pass

    # Dividend yield e payout ratio (valores atuais do info)
    div_yield = None
    payout_ratio = None

    # Book-to-market (usando valores históricos)
    book_to_market = None

    rows.append({
        "Date": date,
        "Year": year,
        "Revenue": revenue,
        "Net Income": net_income,
        "Total Assets": total_assets,
        "Total Equity": total_equity,
        "Total Debt": total_debt,
        "CAPEX": capex,
        "ROE": roe,
        "ROA": roa,
        "ROIC": roic,
        "Operating Margin": operating_margin,
        "EPS": eps,
        "P/E": pe,
        "Dividend Yield": div_yield,
        "Payout Ratio": payout_ratio,
        "Book-to-Market": book_to_market
    })

df = pd.DataFrame(rows)

# Formatar o DataFrame para melhor visualização
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.float_format', lambda x: f'{x:.2f}' if abs(x) < 1 else f'{x:.2e}')

print(f"\n{'='*80}")
print(f"Dados Financeiros Históricos - {TICKER}")
print(f"{'='*80}\n")
print(df.to_string(index=False))
print(f"\n{'='*80}")
print(f"Total de anos com dados: {len(df)}")
print(f"{'='*80}\n")
