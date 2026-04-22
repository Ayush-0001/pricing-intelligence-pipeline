import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup


LOGGER = logging.getLogger(__name__)


@dataclass
class PricingPlan:
    company: str
    plan_name: str
    price_text: str
    billing_cycle: str
    features: List[str]
    source_url: str
    scraped_at: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "company": self.company,
            "plan_name": self.plan_name,
            "price_text": self.price_text,
            "billing_cycle": self.billing_cycle,
            "features": self.features,
            "source_url": self.source_url,
            "scraped_at": self.scraped_at,
        }


class BasePricingScraper:
    def __init__(self, retries: int = 3, timeout: int = 20) -> None:
        self.retries = retries
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            }
        )

    def fetch_html(self, url: str) -> Optional[str]:
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.text
            except requests.RequestException as exc:
                LOGGER.warning("Fetch failed (%s/%s) for %s: %s", attempt, self.retries, url, exc)
                if attempt < self.retries:
                    time.sleep(1.5 * attempt)
        return None

    @staticmethod
    def parse_price_text(text: str) -> str:
        clean = re.sub(r"\s+", " ", text or "").strip()
        return clean or "N/A"

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def soup(html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    def scrape(self) -> List[Dict[str, Any]]:
        raise NotImplementedError


def write_raw_json(records: List[Dict[str, Any]], output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(records, file, indent=2, ensure_ascii=False)
