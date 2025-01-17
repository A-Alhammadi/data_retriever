######################
# PostgreSQL Settings
######################
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "mydatabase"
DB_USER = "myuser"
DB_PASS = "mypassword"

MINCONN = 1
MAXCONN = 10

######################
# Data Settings
######################
# If you want ALL available data from ticker inception, set START_DATE = None
# Otherwise, set something like "2020-01-01"
START_DATE = "2020-01-01"
# START_DATE = None  # Uncomment this if you want earliest possible date

# If you leave END_DATE = None, yfinance fetches through the present day
END_DATE = None
DATA_FETCH_INTERVAL = "1d"

######################
# Tickers
######################
# OPTION A: read from S&P500 Wikipedia
USE_SP500_WIKIPEDIA = True

# OPTION B: define your own ticker list
# TICKERS = ["AAPL", "MSFT"]
# config.py
STOCK_LIST_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


######################
# Sector/ETF Tickers
######################
SECTOR_ETFS = [
    "SPY",  # S&P 500
    "XLE",  # Energy
    "XLF",  # Financials
    "XLI",  # Industrials
    "XLK",  # Technology
    "XLB",  # Materials
    "XLP",  # Consumer Staples
    "XLY",  # Consumer Discretionary
    "XLU",  # Utilities
    "XLV",  # Healthcare
    "XLRE"  # Real Estate
]
######################
# SMA Calculation
######################
MA_SHORT = 50
MA_LONG = 200

######################
# Thread Pool
######################
MAX_WORKERS = 5
