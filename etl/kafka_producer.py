import os
import json
import logging
from typing import List, Dict, Any

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv(dotenv_path=".env")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _serialize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a validated record into a Kafka-safe JSON payload.
    """
    return {
        "ticker": record["ticker"],
        "timestamp": record["timestamp"].isoformat() if record.get("timestamp") else None,
        "open": record["open"],
        "high": record["high"],
        "low": record["low"],
        "close": record["close"],
        "volume": record["volume"],
    }


def get_producer() -> KafkaProducer:
    bootstrap_servers = _get_required_env("KAFKA_BOOTSTRAP_SERVERS")

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def publish_to_kafka(records: List[Dict[str, Any]], topic: str | None = None) -> None:
    """
    Publish validated stock records to Kafka.
    Uses ticker as the message key.
    """
    if not records:
        logger.warning("No records to publish to Kafka")
        return

    kafka_topic = topic or _get_required_env("KAFKA_TOPIC")

    producer = None
    published_count = 0

    try:
        producer = get_producer()

        for record in records:
            payload = _serialize_record(record)
            key = payload["ticker"]

            producer.send(
                topic=kafka_topic,
                key=key,
                value=payload,
            )
            published_count += 1

        producer.flush()
        logger.info(f"Published {published_count} records to Kafka topic '{kafka_topic}'")

    except Exception as e:
        logger.error(f"Kafka publish failed: {e}")
        raise

    finally:
        if producer:
            producer.close()