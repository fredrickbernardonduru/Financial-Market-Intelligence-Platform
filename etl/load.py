import psycopg2
from psycopg2.extras import execute_batch
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)