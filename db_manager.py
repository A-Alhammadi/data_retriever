# db_manager.py

import logging
import psycopg2
import psycopg2.extras
import time
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
import config

logging.basicConfig(level=logging.INFO)

# Retry configuration
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5  # seconds

class DatabasePool:
    _instance = None
    _pool = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            # Attempt to create the connection pool with retries
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    cls._instance._pool = ThreadedConnectionPool(
                        minconn=config.MINCONN,
                        maxconn=config.MAXCONN,
                        host=config.DB_HOST,
                        port=config.DB_PORT,
                        dbname=config.DB_NAME,
                        user=config.DB_USER,
                        password=config.DB_PASS,
                        connect_timeout=10
                    )
                    logging.info("Connection pool created successfully.")
                    break
                except psycopg2.Error as e:
                    logging.error(f"Failed to create connection pool (attempt {attempt+1}): {e}")
                    if attempt < RETRY_ATTEMPTS - 1:
                        time.sleep(RETRY_DELAY)
                    else:
                        logging.error("Failed to create the connection pool after multiple attempts.")
                        raise
        return cls._instance

    @contextmanager
    def get_connection(self):
        conn = None
        for attempt in range(RETRY_ATTEMPTS):
            try:
                conn = self._pool.getconn()
                break
            except psycopg2.Error as e:
                logging.error(f"Database connection error (attempt {attempt+1}): {e}")
                if attempt < RETRY_ATTEMPTS - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    raise

        try:
            yield conn
        except psycopg2.Error as e:
            logging.error(f"Database error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self._pool.putconn(conn)

    def monitor_pool(self):
        if self._pool:
            used_conns = len(self._pool._used)
            free_conns = len(self._pool._pool)
            logging.info(
                f"[DB POOL] Used: {used_conns}, "
                f"Available: {free_conns}, "
                f"Min: {self._pool.minconn}, Max: {self._pool.maxconn}"
            )

    def close_all(self):
        if self._pool:
            self._pool.closeall()
            logging.info("All connections in the pool have been closed.")

db_pool = DatabasePool()


class DBConnectionManager:
    def __enter__(self):
        try:
            self.conn = db_pool._pool.getconn()
            return self.conn
        except psycopg2.Error as e:
            logging.error(f"Failed to connect to DB: {e}")
            self.conn = None
            return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
            db_pool._pool.putconn(self.conn)

        if exc_type:
            logging.error("Exception in DBConnectionManager:", exc_info=(exc_type, exc_val, exc_tb))
            return False
