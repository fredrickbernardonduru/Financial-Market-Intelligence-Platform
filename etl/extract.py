import os
import time
import requests
from typing import Dict, Any
from datetime import datetime

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")
BASE_URL = "https://www.alphavantage.co/query"