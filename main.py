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
    Ensure the table price_data exists with sma_50 and sma_200 columns.
    We attempt to add columns if they do not exist.
    """
    with conn.cursor() as cur:
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS price_data (
            ticker VARCHAR(20),
            trade_date DATE,
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

        # In case the table exists but lacks columns
        alter_sql = """
        ALTER TABLE price_data
        ADD COLUMN IF NOT EXISTS sma_50 NUMERIC,
        ADD COLUMN IF NOT EXISTS sma_200 NUMERIC;
        """
        cur.execute(alter_sql)

        # Index on trade_date
        cur.execute("CREATE INDEX IF NOT EXISTS idx_price_trade_date ON price_data (trade_date);")

        conn.commit()
    logging.info("Ensured price_data table and columns exist.")


def get_sp500_tickers() -> list:
    """
    Returns a list of S&P500 tickers from Wikipedia, or an empty list on failure.
    """
    import requests
    from bs4 import BeautifulSoup

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tickers = []
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            logging.error("Failed to fetch S&P 500 from Wikipedia.")
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", {"id": "constituents"})
        if not table:
            logging.error("No S&P 500 table found on Wikipedia.")
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
    """
    try:
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

        # If there's no 'close', skip
        if "close" not in df.columns:
            logging.warning(f"'{ticker}' => no 'close' column. Possibly invalid ticker.")
            return pd.DataFrame()

        # Ensure DatetimeIndex
        df.index = pd.to_datetime(df.index, errors="coerce")
        # Drop NaN index rows
        df = df[~df.index.isna()]

        return df
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()


def compute_sma(df: pd.DataFrame, sma_short: int, sma_long: int) -> pd.DataFrame:
    """
    Compute and add SMA columns.
    """
    if df.empty:
        return df.copy()

    df = df.copy()
    df.sort_index(inplace=True)

    # Rolling means on the 'close' column
    df[f"sma_{sma_short}"] = df["close"].rolling(window=sma_short, min_periods=1).mean()
    df[f"sma_{sma_long}"] = df["close"].rolling(window=sma_long, min_periods=1).mean()

    return df


def write_to_db(conn, ticker, df: pd.DataFrame):
    """
    Insert or update daily records for a single ticker into price_data table.
    """
    if df.empty:
        logging.warning(f"No data to write for {ticker}.")
        return

    df = df.copy()

    # Reset index -> 'Date' column
    df.reset_index(inplace=True)
    if df.columns[0] == "index":
        df.rename(columns={"index": "Date"}, inplace=True)
    # If no 'Date' column, rename the first column forcibly
    if "Date" not in df.columns:
        df.rename(columns={df.columns[0]: "Date"}, inplace=True)

    if "Date" not in df.columns:
        logging.error(f"Ticker {ticker} => no 'Date' after reset. Skipping.")
        return

    # Convert 'Date' to datetime, drop rows that are NaT
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

            records.append((
                ticker,
                trade_date,
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
        (ticker, trade_date, open, high, low, close, volume, sma_50, sma_200)
    VALUES %s
    ON CONFLICT (ticker, trade_date) DO UPDATE
        SET open = EXCLUDED.open,
            high = EXCLUDED.high,
            low  = EXCLUDED.low,
            close= EXCLUDED.close,
            volume=EXCLUDED.volume,
            sma_50=EXCLUDED.sma_50,
            sma_200=EXCLUDED.sma_200
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

        # Ensure price_data table, columns
        ensure_price_table_columns(conn)

        # Resolve tickers
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
                df_fetched = fetch_data(tkr, config.START_DATE, config.END_DATE)
                if df_fetched.empty:
                    logging.warning(f"{tkr} => no data, skipping.")
                    return

                # Compute SMAs
                df_fetched = compute_sma(df_fetched, config.MA_SHORT, config.MA_LONG)
                if df_fetched.empty:
                    logging.warning(f"{tkr} => empty after SMA, skipping.")
                    return

                with DBConnectionManager() as local_conn:
                    write_to_db(local_conn, tkr, df_fetched)

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
