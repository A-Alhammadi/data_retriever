# config.py

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
START_DATE = "2020-01-01"  # or any date that will yield data
END_DATE = None            # or "2023-12-31", etc.
DATA_FETCH_INTERVAL = "1d"

######################
# Tickers
######################
# OPTION A: read from S&P500 Wikipedia
USE_SP500_WIKIPEDIA = True

# OPTION B: define your own ticker list
# TICKERS = ["AAPL", "MSFT"]

######################
# SMA Calculation
######################
MA_SHORT = 50
MA_LONG = 200

######################
# Thread Pool
######################
MAX_WORKERS = 5
