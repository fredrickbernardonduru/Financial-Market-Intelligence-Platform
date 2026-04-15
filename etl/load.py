import psycopg2
from psycopg2.extras import execute_batch
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        db_name="your_db",
        password = "your_password"
    )

def load_to_postgres(records: List[Dict[str,Any]]):
    """
    Bulk ;oad records into PostgreSQL with UPSERT logic
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

