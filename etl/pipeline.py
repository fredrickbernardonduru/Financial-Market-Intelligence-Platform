import logging
import sys
import os


from etl.extract import AlphaVantageClient
from etl.clean import normalize_daily_series
from etl.validate import validate_batch

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_pipeline(symbol: str):
    """
    Simple pipeline:
    extract → clean → validate
    """

    try:
        # -------------------------------
        # 🔹 Extract
        # -------------------------------
        logger.info(f"Starting extraction for {symbol}")
        client = AlphaVantageClient()
        raw_data = client.get_intraday(symbol)

        # -------------------------------
        # 🔹 Clean / Normalize
        # -------------------------------
        logger.info("Normalizing data")
        records = normalize_daily_series(raw_data)

        logger.info(f"Extracted {len(records)} records")

        # -------------------------------
        # 🔹 Validate
        # -------------------------------
        logger.info("Validating data")
        valid_records = validate_batch(records)

        logger.info(f"Valid records: {len(valid_records)}")

        # -------------------------------
        # 🔹 Sample Output
        # -------------------------------
        if valid_records:
            logger.info("Sample record:")
            logger.info(valid_records[0])

        return valid_records

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    # 🔥 Start with ONE ticker
    symbol = "AAPL"

    results = run_pipeline(symbol)

    print(f"\nPipeline finished. Total valid records: {len(results)}")