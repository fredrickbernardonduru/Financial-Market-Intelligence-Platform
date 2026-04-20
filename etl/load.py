import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch
import logging
from typing import List, Dict, Any

load_dotenv(dotenv_path=".env")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_connection():

    print("\n🔍 DEBUG ENV VALUES")
    print("HOST:", repr(os.getenv("DB_HOST")))
    print("PORT:", repr(os.getenv("DB_PORT")))
    print("DB:", repr(os.getenv("DB_NAME")))
    print("USER:", repr(os.getenv("DB_USER")))
    print("PASSWORD:", repr(os.getenv("DB_PASSWORD")))
    print()

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def load_to_postgres(records: List[Dict[str, Any]]):
    """
    Bulk load records into PostgreSQL with UPSERT logic
    """

    if not records:
        logger.warning("No records to load")
        return

    conn = get_connection()
    cursor = conn.cursor()

    query = """
        INSERT INTO bronze_stock_ticks (
            ticker, timestamp, open, high, low, close, volume
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, timestamp)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume;
    """

    data = [
        (
            r["ticker"],
            r["timestamp"],
            r["open"],
            r["high"],
            r["low"],
            r["close"],
            r["volume"],
        )
        for r in records
    ]

    try:
        execute_batch(cursor, query, data, page_size=100)
        conn.commit()

        logger.info(f"Loaded {len(records)} records into PostgreSQL")

    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed: {e}")
        raise

    finally:
        cursor.close()
        conn.close()