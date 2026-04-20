import logging

from etl.extract import AlphaVantageClient
from etl.clean import normalize_daily_series
from etl.validate import validate_batch
from etl.load import load_to_postgres

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_pipeline(symbol: str):
    """
    Run the ETL pipeline for a single ticker symbol:
    extract -> clean -> validate -> load
    """
    try:
        logger.info(f"Starting extraction for {symbol}")
        client = AlphaVantageClient()
        raw_data = client.get_intraday(symbol)

        logger.info(f"Normalizing data for {symbol}")
        records = normalize_daily_series(raw_data)
        logger.info(f"Extracted {len(records)} normalized records for {symbol}")

        logger.info(f"Validating data for {symbol}")
        valid_records = validate_batch(records)
        logger.info(f"Valid records for {symbol}: {len(valid_records)}")

        if valid_records:
            logger.info(f"Sample record for {symbol}: {valid_records[0]}")

        logger.info(f"Loading valid records for {symbol} into PostgreSQL")
        load_to_postgres(valid_records)

        logger.info(f"Pipeline finished successfully for {symbol}")
        return valid_records

    except Exception as e:
        logger.error(f"Pipeline failed for {symbol}: {e}")
        raise


if __name__ == "__main__":
    symbol = "AAPL"
    results = run_pipeline(symbol)
    logger.info(f"Total valid records processed for {symbol}: {len(results)}")