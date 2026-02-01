"""
AlphaVantage PostgreSQL Database Setup
Creates database schema and populates ticker table from merged_listings.csv
"""

import os
import sys
import locale

# Set Python's default encoding
if sys.platform == 'win32':
    # On Windows, ensure we use UTF-8
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Set encoding environment variables before importing psycopg2
os.environ['PGCLIENTENCODING'] = 'UTF8'
os.environ['PYTHONIOENCODING'] = 'utf-8'
# Disable password file and service file to avoid encoding issues
os.environ['PGPASSFILE'] = 'nul'  # Windows null device
os.environ['PGSERVICEFILE'] = 'nul'
# Disable system config directory
os.environ['PGSYSCONFDIR'] = os.getcwd()
# Force locale
os.environ['LC_ALL'] = 'C.UTF-8'
os.environ['LANG'] = 'C.UTF-8'

import psycopg2
from psycopg2 import sql
import csv
from datetime import datetime
import traceback


class AlphaVantageDB:
    def __init__(self, dbname='alphavantage', user='postgres', password='pgadmin',
                 host='localhost', port='5433'):
        """Initialize database connection parameters"""
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cursor = None

    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            # Use connection string to avoid encoding issues
            conn_string = f"host={self.host} port={self.port} dbname={self.dbname} user={self.user} password={self.password} client_encoding=UTF8"
            self.conn = psycopg2.connect(conn_string)
            self.cursor = self.conn.cursor()

            # Get PostgreSQL version
            self.cursor.execute("SELECT version();")
            version = self.cursor.fetchone()[0]
            pg_version = version.split()[1] if len(version.split()) > 1 else "unknown"

            print(f"Successfully connected to database: {self.dbname}")
            print(f"PostgreSQL version: {pg_version}")
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            raise

    def create_database(self):
        """Create database if it doesn't exist"""
        try:
            # Connect to default postgres database to create new database
            print(f"Attempting connection to PostgreSQL...")

            # Try connection with explicit parameters to isolate encoding issue
            try:
                # Method 1: Using parameters dictionary
                conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    dbname='postgres',
                    user=self.user,
                    password=self.password
                )
            except UnicodeDecodeError as ude:
                print(f"UnicodeDecodeError with dict params: {ude}")
                print(f"Trying alternative connection method...")
                # Method 2: Minimal connection string
                try:
                    import urllib.parse
                    # URL encode the password in case it has special characters
                    encoded_password = urllib.parse.quote_plus(self.password)
                    conn_url = f"postgresql://{self.user}:{encoded_password}@{self.host}:{self.port}/postgres"
                    conn = psycopg2.connect(conn_url)
                except Exception as e2:
                    print(f"Alternative method failed: {e2}")
                    raise ude

            conn.set_client_encoding('UTF8')
            conn.autocommit = True
            cursor = conn.cursor()

            # Check if database exists
            cursor.execute(
                "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
                (self.dbname,)
            )
            exists = cursor.fetchone()

            if not exists:
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(self.dbname)
                ))
                print(f"Database '{self.dbname}' created successfully")
            else:
                print(f"Database '{self.dbname}' already exists")

            cursor.close()
            conn.close()
        except psycopg2.Error as e:
            print(f"Error creating database: {e}")
            raise

    def create_tables(self):
        """Create all required tables"""
        try:
            # Create ticker table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS ticker (
                    ticker_id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) UNIQUE NOT NULL,
                    name VARCHAR(255),
                    exchange VARCHAR(50),
                    asset_type VARCHAR(50),
                    ipo_date DATE,
                    delisting_date DATE,
                    status VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            print("Created table: ticker")

            # Create balance_sheet table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS balance_sheet (
                    balance_sheet_id SERIAL PRIMARY KEY,
                    ticker_id INTEGER REFERENCES ticker(ticker_id) ON DELETE CASCADE,
                    fiscal_date_ending DATE NOT NULL,
                    reported_currency VARCHAR(10),
                    total_assets BIGINT,
                    total_current_assets BIGINT,
                    cash_and_cash_equivalents_at_carrying_value BIGINT,
                    cash_and_short_term_investments BIGINT,
                    inventory BIGINT,
                    current_net_receivables BIGINT,
                    total_non_current_assets BIGINT,
                    property_plant_equipment BIGINT,
                    accumulated_depreciation_amortization_ppe BIGINT,
                    intangible_assets BIGINT,
                    intangible_assets_excluding_goodwill BIGINT,
                    goodwill BIGINT,
                    investments BIGINT,
                    long_term_investments BIGINT,
                    short_term_investments BIGINT,
                    other_current_assets BIGINT,
                    other_non_current_assets BIGINT,
                    total_liabilities BIGINT,
                    total_current_liabilities BIGINT,
                    current_accounts_payable BIGINT,
                    deferred_revenue BIGINT,
                    current_debt BIGINT,
                    short_term_debt BIGINT,
                    total_non_current_liabilities BIGINT,
                    capital_lease_obligations BIGINT,
                    long_term_debt BIGINT,
                    current_long_term_debt BIGINT,
                    long_term_debt_noncurrent BIGINT,
                    short_long_term_debt_total BIGINT,
                    other_current_liabilities BIGINT,
                    other_non_current_liabilities BIGINT,
                    total_shareholder_equity BIGINT,
                    treasury_stock BIGINT,
                    retained_earnings BIGINT,
                    common_stock BIGINT,
                    common_stock_shares_outstanding BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker_id, fiscal_date_ending)
                )
            """)
            print("Created table: balance_sheet")

            # Create cashflow table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS cashflow (
                    cashflow_id SERIAL PRIMARY KEY,
                    ticker_id INTEGER REFERENCES ticker(ticker_id) ON DELETE CASCADE,
                    fiscal_date_ending DATE NOT NULL,
                    reported_currency VARCHAR(10),
                    operating_cashflow BIGINT,
                    payments_for_operating_activities BIGINT,
                    proceeds_from_operating_activities BIGINT,
                    change_in_operating_liabilities BIGINT,
                    change_in_operating_assets BIGINT,
                    depreciation_depletion_and_amortization BIGINT,
                    capital_expenditures BIGINT,
                    change_in_receivables BIGINT,
                    change_in_inventory BIGINT,
                    profit_loss BIGINT,
                    cashflow_from_investment BIGINT,
                    cashflow_from_financing BIGINT,
                    proceeds_from_repayments_of_short_term_debt BIGINT,
                    payments_for_repurchase_of_common_stock BIGINT,
                    payments_for_repurchase_of_equity BIGINT,
                    payments_for_repurchase_of_preferred_stock BIGINT,
                    dividend_payout BIGINT,
                    dividend_payout_common_stock BIGINT,
                    dividend_payout_preferred_stock BIGINT,
                    proceeds_from_issuance_of_common_stock BIGINT,
                    proceeds_from_issuance_of_long_term_debt_and_capital_securities_net BIGINT,
                    proceeds_from_issuance_of_preferred_stock BIGINT,
                    proceeds_from_repurchase_of_equity BIGINT,
                    proceeds_from_sale_of_treasury_stock BIGINT,
                    change_in_cash_and_cash_equivalents BIGINT,
                    change_in_exchange_rate BIGINT,
                    net_income BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker_id, fiscal_date_ending)
                )
            """)
            print("Created table: cashflow")

            # Create income_statement table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS income_statement (
                    income_statement_id SERIAL PRIMARY KEY,
                    ticker_id INTEGER REFERENCES ticker(ticker_id) ON DELETE CASCADE,
                    fiscal_date_ending DATE NOT NULL,
                    reported_currency VARCHAR(10),
                    gross_profit BIGINT,
                    total_revenue BIGINT,
                    cost_of_revenue BIGINT,
                    cost_of_goods_and_services_sold BIGINT,
                    operating_income BIGINT,
                    selling_general_and_administrative BIGINT,
                    research_and_development BIGINT,
                    operating_expenses BIGINT,
                    investment_income_net BIGINT,
                    net_interest_income BIGINT,
                    interest_income BIGINT,
                    interest_expense BIGINT,
                    non_interest_income BIGINT,
                    other_non_operating_income BIGINT,
                    depreciation BIGINT,
                    depreciation_and_amortization BIGINT,
                    income_before_tax BIGINT,
                    income_tax_expense BIGINT,
                    interest_and_debt_expense BIGINT,
                    net_income_from_continuing_operations BIGINT,
                    comprehensive_income_net_of_tax BIGINT,
                    ebit BIGINT,
                    ebitda BIGINT,
                    net_income BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker_id, fiscal_date_ending)
                )
            """)
            print("Created table: income_statement")

            # Create indexes for better query performance
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_ticker_symbol ON ticker(symbol);
            """)
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_balance_sheet_ticker ON balance_sheet(ticker_id);
            """)
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_balance_sheet_date ON balance_sheet(fiscal_date_ending);
            """)
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_cashflow_ticker ON cashflow(ticker_id);
            """)
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_cashflow_date ON cashflow(fiscal_date_ending);
            """)
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_income_statement_ticker ON income_statement(ticker_id);
            """)
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_income_statement_date ON income_statement(fiscal_date_ending);
            """)
            print("Created indexes")

            self.conn.commit()
            print("All tables created successfully")
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"Error creating tables: {e}")
            raise

    def load_tickers_from_csv(self, csv_file='merged_listings.csv'):
        """Load ticker data from merged_listings.csv"""
        if not os.path.exists(csv_file):
            print(f"Error: File '{csv_file}' not found")
            return

        # Try multiple encodings to handle various CSV formats
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        file_handle = None
        csv_reader = None

        for encoding in encodings:
            try:
                file_handle = open(csv_file, 'r', encoding=encoding, errors='replace')
                csv_reader = csv.reader(file_handle)
                # Test reading first line
                next(csv_reader, None)
                # Reset to beginning
                file_handle.seek(0)
                csv_reader = csv.reader(file_handle)
                print(f"Successfully opened CSV with encoding: {encoding}")
                break
            except (UnicodeDecodeError, Exception) as e:
                if file_handle:
                    file_handle.close()
                if encoding == encodings[-1]:
                    print(f"Error: Could not read CSV with any supported encoding")
                    return
                continue

        try:
            inserted_count = 0
            skipped_count = 0

            for row in csv_reader:
                if len(row) < 4:
                    continue

                symbol = row[0].strip() if row[0] else None
                name = row[1].strip() if row[1] else None
                exchange = row[2].strip() if row[2] else None
                asset_type = row[3].strip() if row[3] else None
                ipo_date = row[4].strip() if len(row) > 4 and row[4] else None
                delisting_date = row[5].strip() if len(row) > 5 and row[5] else None
                status = row[6].strip() if len(row) > 6 and row[6] else 'Active'

                if not symbol:
                    continue

                # Convert date strings to date objects
                ipo_date_obj = None
                if ipo_date:
                    try:
                        ipo_date_obj = datetime.strptime(ipo_date, '%Y-%m-%d').date()
                    except ValueError:
                        pass

                delisting_date_obj = None
                if delisting_date:
                    try:
                        delisting_date_obj = datetime.strptime(delisting_date, '%Y-%m-%d').date()
                    except ValueError:
                        pass

                try:
                    self.cursor.execute("""
                        INSERT INTO ticker 
                        (symbol, name, exchange, asset_type, ipo_date, delisting_date, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol) DO UPDATE SET
                            name = EXCLUDED.name,
                            exchange = EXCLUDED.exchange,
                            asset_type = EXCLUDED.asset_type,
                            ipo_date = EXCLUDED.ipo_date,
                            delisting_date = EXCLUDED.delisting_date,
                            status = EXCLUDED.status,
                            updated_at = CURRENT_TIMESTAMP
                    """, (symbol, name, exchange, asset_type, ipo_date_obj,
                          delisting_date_obj, status))
                    inserted_count += 1

                    if inserted_count % 1000 == 0:
                        self.conn.commit()
                        print(f"Inserted {inserted_count} tickers...")
                except psycopg2.Error as e:
                    print(f"Error inserting ticker {symbol}: {e}")
                    skipped_count += 1
                    continue

            self.conn.commit()
            print(f"\nTicker data loading complete:")
            print(f"  - Inserted/Updated: {inserted_count}")
            print(f"  - Skipped: {skipped_count}")
        except Exception as e:
            self.conn.rollback()
            print(f"Error loading tickers from CSV: {e}")
            raise
        finally:
            if file_handle:
                file_handle.close()

    def get_ticker_count(self):
        """Get total count of tickers in database"""
        try:
            self.cursor.execute("SELECT COUNT(*) FROM ticker")
            count = self.cursor.fetchone()[0]
            return count
        except psycopg2.Error as e:
            print(f"Error getting ticker count: {e}")
            return 0

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Database connection closed")


def main():
    """Main execution function"""
    # Database configuration - modify these as needed
    DB_CONFIG = {
        'dbname': 'alphavantage',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '5433'
    }

    CSV_FILE = 'merged_listings.csv'

    print("=" * 60)
    print("AlphaVantage Database Setup")
    print("=" * 60)

    db = AlphaVantageDB(**DB_CONFIG)

    try:
        # Create database
        print("\n[1/4] Creating database...")
        db.create_database()

        # Connect to database
        print("\n[2/4] Connecting to database...")
        db.connect()

        # Create tables
        print("\n[3/4] Creating tables...")
        db.create_tables()

        # Load ticker data
        print("\n[4/4] Loading ticker data from CSV...")
        db.load_tickers_from_csv(CSV_FILE)

        # Show summary
        print("\n" + "=" * 60)
        print("Setup Complete!")
        print("=" * 60)
        ticker_count = db.get_ticker_count()
        print(f"Total tickers in database: {ticker_count}")
        print("\nTables created:")
        print("  - ticker")
        print("  - balance_sheet")
        print("  - cashflow")
        print("  - income_statement")

    except Exception as e:
        print(f"\nSetup failed: {e}")
        print("\nFull error traceback:")
        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    main()

