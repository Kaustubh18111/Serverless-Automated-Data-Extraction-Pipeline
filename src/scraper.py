from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_retry_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_page(session: requests.Session, url: str, timeout_seconds: int) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; ServerlessDataExtractionPipeline/2.0; "
            "+https://aws.amazon.com/lambda/)"
        )
    }
    response = session.get(url, headers=headers, timeout=timeout_seconds)
    response.raise_for_status()
    return response.text


def _parse_books_page(
    html: str,
    page_url: str,
    page_number: int,
) -> tuple[list[dict[str, Any]], str | None]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("article.product_pod")

    items: list[dict[str, Any]] = []
    for index, card in enumerate(cards):
        title_tag = card.select_one("h3 a")
        price_tag = card.select_one(".price_color")
        availability_tag = card.select_one(".availability")

        href = str(title_tag.get("href")) if title_tag and title_tag.get("href") else None
        title_value = title_tag.get("title") if title_tag else ""
        title = (
            title_value.strip()
            if isinstance(title_value, str)
            else str(title_value or "").strip()
        )

        item = {
            "title": title,
            "price": price_tag.get_text(strip=True) if price_tag else "",
            "availability": (
                " ".join(availability_tag.get_text(" ", strip=True).split())
                if availability_tag
                else ""
            ),
            "product_url": urljoin(page_url, href) if href else page_url,
            "page_number": page_number,
            "position": index,
        }
        items.append(item)

    next_link = soup.select_one("li.next a")
    next_href = str(next_link.get("href")) if next_link and next_link.get("href") else None
    next_url = urljoin(page_url, next_href) if next_href else None
    return items, next_url


def scrape_items(
    source_url: str,
    max_pages: int,
    timeout_seconds: int,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    active_session = session or build_retry_session()
    items: list[dict[str, Any]] = []
    current_url: str | None = source_url
    page_number = 1

    while current_url and page_number <= max_pages:
        html = fetch_page(active_session, current_url, timeout_seconds)
        page_items, next_url = _parse_books_page(html, current_url, page_number)
        items.extend(page_items)
        current_url = next_url
        page_number += 1

    return items
