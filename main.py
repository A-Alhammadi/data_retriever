# main.py
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import yfinance as yf
import psycopg2.extras
import config
from db_manager import DBConnectionManager, db_pool

logging.basicConfig(level=logging.INFO)

def ensure_price_table_columns(conn):
    """
    Ensure the table price_data exists with columns for sector, sma_50, and sma_200.
    """
    with conn.cursor() as cur:
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS price_data (
            ticker VARCHAR(20),
            trade_date DATE,
            sector TEXT,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume BIGINT,
            sma_50 NUMERIC,
            sma_200 NUMERIC,
            PRIMARY KEY (ticker, trade_date)
        );
        """
        cur.execute(create_table_sql)

        alter_sql = """
        ALTER TABLE price_data
        ADD COLUMN IF NOT EXISTS sector TEXT,
        ADD COLUMN IF NOT EXISTS sma_50 NUMERIC,
        ADD COLUMN IF NOT EXISTS sma_200 NUMERIC;
        """
        cur.execute(alter_sql)

        # Index on trade_date
        cur.execute("CREATE INDEX IF NOT EXISTS idx_price_trade_date ON price_data (trade_date);")

        conn.commit()
    logging.info("Ensured price_data table, including 'sector', 'sma_50', and 'sma_200' columns.")


def get_sp500_tickers() -> list:
    """
    Returns a list of S&P500 tickers from Wikipedia, using the URL from config.STOCK_LIST_URL.
    """
    import requests
    from bs4 import BeautifulSoup

    url = config.STOCK_LIST_URL  # moved from inline to config
    tickers = []
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            logging.error("Failed to fetch S&P 500 from Wikipedia.")
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", {"id": "constituents"})
        if not table:
            logging.error("No S&P 500 table found on Wikipedia page.")
            return []
        rows = table.find_all("tr")[1:]
        for row in rows:
            cols = row.find_all("td")
            if cols:
                ticker = cols[0].text.strip()
                ticker = ticker.replace(".", "-")
                tickers.append(ticker)
    except Exception as e:
        logging.error(f"Error fetching S&P 500 tickers: {e}")
    return tickers


def fetch_data(ticker: str, start_date, end_date) -> pd.DataFrame:
    """
    Fetch OHLCV from Yahoo Finance for the given ticker and date range.
    If start_date is None, fetch the max history available.
    Otherwise, fetch data from start_date to end_date.
    """
    try:
        if start_date is None:
            df = yf.download(
                tickers=ticker,
                period="max",
                interval=config.DATA_FETCH_INTERVAL,
                progress=False
            )
        else:
            df = yf.download(
                tickers=ticker,
                start=start_date,
                end=end_date,
                interval=config.DATA_FETCH_INTERVAL,
                progress=False
            )

        if df.empty:
            logging.warning(f"No data returned for {ticker}.")
            return pd.DataFrame()

        # Flatten multi-index columns if needed
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]

        # Drop 'Adj Close' if present
        if "Adj Close" in df.columns:
            df.drop(columns=["Adj Close"], inplace=True, errors="ignore")

        # Rename columns
        rename_map = {
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        }
        df.rename(columns=rename_map, inplace=True)

        if "close" not in df.columns:
            logging.warning(f"'{ticker}' => no 'close' column. Possibly invalid ticker.")
            return pd.DataFrame()

        # Ensure DatetimeIndex
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[~df.index.isna()]  # Drop NaN index rows

        return df
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}", exc_info=True)
        return pd.DataFrame()


def get_sector_for_ticker(ticker: str) -> str:
    """
    Retrieve the sector for a ticker via yfinance. 
    Returns "Unknown" if not found.
    """
    try:
        info = yf.Ticker(ticker).info
        sector_val = info.get("sector", None)
        if not sector_val:
            return "Unknown"
        return sector_val
    except Exception as e:
        logging.warning(f"Could not retrieve sector for {ticker}: {e}")
        return "Unknown"


def compute_sma(df: pd.DataFrame, sma_short: int, sma_long: int) -> pd.DataFrame:
    """
    Compute and add SMA columns to the DataFrame.
    """
    if df.empty:
        return df.copy()

    df = df.copy()
    df.sort_index(inplace=True)

    df[f"sma_{sma_short}"] = df["close"].rolling(window=sma_short, min_periods=1).mean()
    df[f"sma_{sma_long}"] = df["close"].rolling(window=sma_long, min_periods=1).mean()

    return df


def write_to_db(conn, ticker, df: pd.DataFrame, sector_val: str):
    """
    Insert or update daily records for a single ticker into price_data table,
    including the sector field.
    """
    if df.empty:
        logging.warning(f"No data to write for {ticker}.")
        return

    df = df.copy()
    df.reset_index(inplace=True)
    if df.columns[0] == "index":
        df.rename(columns={"index": "Date"}, inplace=True)
    if "Date" not in df.columns:
        df.rename(columns={df.columns[0]: "Date"}, inplace=True)

    if "Date" not in df.columns:
        logging.error(f"Ticker {ticker} => no 'Date' after reset. Skipping.")
        return

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df.dropna(subset=["Date"], inplace=True)
    if df.empty:
        logging.warning(f"No valid rows for {ticker} after date cleanup.")
        return

    records = []
    for _, row in df.iterrows():
        try:
            trade_date = row["Date"].date()
            open_val = float(row["open"]) if not pd.isna(row["open"]) else None
            high_val = float(row["high"]) if not pd.isna(row["high"]) else None
            low_val = float(row["low"]) if not pd.isna(row["low"]) else None
            close_val = float(row["close"]) if not pd.isna(row["close"]) else None
            volume_val = int(row["volume"]) if not pd.isna(row["volume"]) else None
            sma50_val = float(row["sma_50"]) if "sma_50" in row and not pd.isna(row["sma_50"]) else None
            sma200_val = float(row["sma_200"]) if "sma_200" in row and not pd.isna(row["sma_200"]) else None

            # The same sector for all rows for this ticker
            records.append((
                ticker,
                trade_date,
                sector_val,
                open_val,
                high_val,
                low_val,
                close_val,
                volume_val,
                sma50_val,
                sma200_val
            ))
        except Exception as e:
            logging.error(f"Error creating record for {ticker}: {e}")

    if not records:
        logging.warning(f"No valid records after iteration for {ticker}.")
        return

    insert_sql = """
    INSERT INTO price_data
        (ticker, trade_date, sector, open, high, low, close, volume, sma_50, sma_200)
    VALUES %s
    ON CONFLICT (ticker, trade_date) DO UPDATE
        SET sector = EXCLUDED.sector,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            sma_50 = EXCLUDED.sma_50,
            sma_200 = EXCLUDED.sma_200
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, insert_sql, records, page_size=100)
    conn.commit()
    logging.info(f"Inserted/updated {len(records)} rows for {ticker}.")


def main():
    logging.info("Starting data retriever...")

    with DBConnectionManager() as conn:
        if conn is None:
            logging.error("Failed to connect to DB. Exiting.")
            sys.exit(1)

        db_pool.monitor_pool()

        # Ensure price_data table and columns
        ensure_price_table_columns(conn)

        # Resolve tickers: either from Wikipedia S&P 500 list or from config
        if config.USE_SP500_WIKIPEDIA:
            tickers = get_sp500_tickers()
        else:
            tickers = config.TICKERS

        if not tickers:
            logging.error("No tickers found. Exiting.")
            sys.exit(1)

        logging.info(f"Total tickers: {len(tickers)}")

        def process_ticker(tkr):
            try:
                # 1) Fetch daily OHLC data
                df_fetched = fetch_data(tkr, config.START_DATE, config.END_DATE)
                if df_fetched.empty:
                    logging.warning(f"{tkr} => no data, skipping.")
                    return

                # 2) Compute SMAs
                df_fetched = compute_sma(df_fetched, config.MA_SHORT, config.MA_LONG)
                if df_fetched.empty:
                    logging.warning(f"{tkr} => empty after SMA, skipping.")
                    return

                # 3) Retrieve sector from Yahoo Finance
                sector_val = get_sector_for_ticker(tkr)

                # 4) Insert/Update into DB
                with DBConnectionManager() as local_conn:
                    write_to_db(local_conn, tkr, df_fetched, sector_val)

            except Exception as e:
                logging.error(f"Error processing {tkr}: {e}", exc_info=True)

        max_workers = min(config.MAX_WORKERS, len(tickers))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_ticker, tkr) for tkr in tickers]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    logging.error(f"Exception in thread: {e}", exc_info=True)

        db_pool.monitor_pool()
        logging.info("Data retrieval completed successfully.")

if __name__ == "__main__":
    main()
