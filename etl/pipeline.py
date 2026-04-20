import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from etl.extract import AlphaVantageClient
from etl.clean import normalize_daily_series
from etl.validate import validate_batch
from etl.load import load_to_postgres

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_pipeline(symbol: str):
    try:
        logger.info(f"Starting extraction for {symbol}")
        client = AlphaVantageClient()
        raw_data = client.get_intraday(symbol)

        logger.info("Normalizing data")
        records = normalize_daily_series(raw_data)

        logger.info(f"Extracted {len(records)} records")

        logger.info("Validating data")
        valid_records = validate_batch(records)

        logger.info(f"Valid records: {len(valid_records)}")

        if valid_records:
            logger.info(f"Sample record: {valid_records[0]}")

        return valid_records

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    symbol = "AAPL"

    results = run_pipeline(symbol)

    logger.info(f"Pipeline finished. Total valid records: {len(results)}")

    load_to_postgres(results)