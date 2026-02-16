import json
import re
import time
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

COL_TITLE = "\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435"
COL_PRICE = "\u0421\u0442\u043e\u0438\u043c\u043e\u0441\u0442\u044c"
COL_LINK = "\u0421\u0441\u044b\u043b\u043a\u0430"
COL_ROOMS = "\u041a\u043e\u043b-\u0432\u043e \u043a\u043e\u043c\u043d\u0430\u0442"

MAP_URL = (
    "https://krisha.kz/map/prodazha/kvartiry/shymkent/?das[price][to]=17000000&zoom=14&lat=42.31622&lon=69.57153"
    "&areas=p42.326920,69.563423,42.333034,69.569775,42.335963,69.576126,42.338001,69.585739"
    ",42.337619,69.595352,42.335581,69.602905,42.332015,69.607369,42.327557,69.609085"
    ",42.318003,69.608399,42.313289,69.605309,42.310741,69.601360,42.308575,69.589172"
    ",42.309339,69.577328,42.312652,69.567371,42.316347,69.562737,42.321825,69.560505"
    ",42.326793,69.562050,42.326920,69.563423"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}

MAX_PRICE = 16_000_000
TARGET_ROOMS = 2
SEEN_IDS_FILE = Path("seen_ids.json")
TELEGRAM_BOT_TOKEN = ""
TELEGRAM_CHAT_ID = "-5128105376"

REQUEST_TIMEOUT = 30
REQUEST_RETRIES = 3
RETRY_BACKOFF_SEC = 2
REQUEST_DELAY_SEC = 0.7
MAX_CONSECUTIVE_FAILURES = 5
MAX_PAGES = None


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()


def extract_rooms(text: str):
    text = (text or "").lower()
    m = re.search(r"^\s*(\d+)\s*-", text)
    return int(m.group(1)) if m else None


def extract_ad_id(link: str):
    m = re.search(r"/a/show/(\d+)", link)
    return m.group(1) if m else None


def parse_price_to_int(price_text: str):
    digits = re.sub(r"\D", "", price_text or "")
    return int(digits) if digits else None


def build_list_url_from_map(map_url: str) -> str:
    parsed = urlparse(map_url)
    qs = parse_qs(parsed.query)

    areas = (qs.get("areas") or [""])[0]
    if not areas:
        raise ValueError("\u0412 map-\u0441\u0441\u044b\u043b\u043a\u0435 \u043e\u0442\u0441\u0443\u0442\u0441\u0442\u0432\u0443\u0435\u0442 \u043f\u0430\u0440\u0430\u043c\u0435\u0442\u0440 areas")

    list_params = {"areas": areas}
    for key, values in qs.items():
        if key.startswith("das[") and values:
            list_params[key] = values[0]

    return f"https://krisha.kz/prodazha/kvartiry/shymkent/?{urlencode(list_params)}"


def build_page_url(base_list_url: str, page: int) -> str:
    if page <= 1:
        return base_list_url
    sep = "&" if "?" in base_list_url else "?"
    return f"{base_list_url}{sep}page={page}"


def parse_page(html: str):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen_links = set()

    for a in soup.select('a.a-card__title[href*="/a/show/"]'):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        link = urljoin("https://krisha.kz", href)
        if link in seen_links:
            continue

        title = clean_text(a.get("title") or a.get_text(" ", strip=True))
        if not title:
            continue

        card = a.find_parent(lambda tag: tag.name == "div" and "a-card" in (tag.get("class") or []))
        if not card:
            continue

        price_el = card.select_one(".a-card__price, .a-card__price-text")
        price_text = clean_text(price_el.get_text(" ", strip=True)) if price_el else ""

        rooms = extract_rooms(title)
        if rooms is None:
            rooms = extract_rooms(clean_text(card.get_text(" ", strip=True)))

        if not price_text or rooms is None:
            continue

        items.append(
            {
                COL_TITLE: title,
                COL_PRICE: price_text,
                COL_LINK: link,
                COL_ROOMS: rooms,
            }
        )
        seen_links.add(link)

    return items


def get_with_retries(session: requests.Session, url: str):
    last_exc = None
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            print(f"Request failed (attempt {attempt}/{REQUEST_RETRIES}): {url}")
            if attempt < REQUEST_RETRIES:
                time.sleep(RETRY_BACKOFF_SEC * attempt)
    raise last_exc


def scrape_all(max_pages: int | None = None):
    list_url = build_list_url_from_map(MAP_URL)

    session = requests.Session()
    session.headers.update(HEADERS)

    all_items = []
    page = 1
    consecutive_failures = 0

    while True:
        if max_pages is not None and page > max_pages:
            break

        page_url = build_page_url(list_url, page)
        print(f"\u041f\u0430\u0440\u0441\u0438\u043d\u0433: {page_url}")

        try:
            response = get_with_retries(session, page_url)
            consecutive_failures = 0
        except requests.RequestException as exc:
            consecutive_failures += 1
            print(f"\u041f\u0440\u043e\u043f\u0443\u0441\u043a \u0441\u0442\u0440\u0430\u043d\u0438\u0446\u044b \u0438\u0437-\u0437\u0430 \u043e\u0448\u0438\u0431\u043a\u0438 \u0441\u0435\u0442\u0438: {page_url}. \u041e\u0448\u0438\u0431\u043a\u0430: {exc}")
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print("\u0421\u043b\u0438\u0448\u043a\u043e\u043c \u043c\u043d\u043e\u0433\u043e \u043f\u043e\u0434\u0440\u044f\u0434 \u0441\u0435\u0442\u0435\u0432\u044b\u0445 \u043e\u0448\u0438\u0431\u043e\u043a. \u041e\u0441\u0442\u0430\u043d\u0430\u0432\u043b\u0438\u0432\u0430\u0435\u043c \u043f\u0430\u0433\u0438\u043d\u0430\u0446\u0438\u044e.")
                break
            page += 1
            continue

        page_items = parse_page(response.text)
        print(f"\u041d\u0430\u0439\u0434\u0435\u043d\u043e \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u0439 \u043d\u0430 \u0441\u0442\u0440\u0430\u043d\u0438\u0446\u0435: {len(page_items)}")
        if not page_items:
            print("\u041f\u0443\u0441\u0442\u0430\u044f \u0441\u0442\u0440\u0430\u043d\u0438\u0446\u0430, \u043e\u0441\u0442\u0430\u043d\u043e\u0432\u043a\u0430 \u043f\u0430\u0433\u0438\u043d\u0430\u0446\u0438\u0438.")
            break

        all_items.extend(page_items)
        page += 1
        time.sleep(REQUEST_DELAY_SEC)

    rows = list({row[COL_LINK]: row for row in all_items}.values())
    if not rows:
        raise RuntimeError("\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u0438\u0437\u0432\u043b\u0435\u0447\u044c \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u044f \u0432 \u0432\u044b\u0431\u0440\u0430\u043d\u043d\u043e\u0439 \u043e\u0431\u043b\u0430\u0441\u0442\u0438.")

    return rows


def filter_target(rows: list[dict]):
    filtered = []
    for row in rows:
        if row.get(COL_ROOMS) != TARGET_ROOMS:
            continue

        price_int = parse_price_to_int(row.get(COL_PRICE, ""))
        if price_int is None or price_int > MAX_PRICE:
            continue

        ad_id = extract_ad_id(row.get(COL_LINK, ""))
        if not ad_id:
            continue

        row_copy = dict(row)
        row_copy["ad_id"] = ad_id
        row_copy["price_int"] = price_int
        filtered.append(row_copy)

    return filtered


def load_seen_ids(path: Path):
    if not path.exists():
        return set()

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return set()

    return {str(x) for x in data} if isinstance(data, list) else set()


def save_seen_ids(path: Path, ids: set[str]):
    path.write_text(json.dumps(sorted(ids), ensure_ascii=False, indent=2), encoding="utf-8")


def split_messages(lines: list[str], limit: int = 3500):
    chunks = []
    current = ""
    for line in lines:
        block = line + "\n\n"
        if len(current) + len(block) > limit and current:
            chunks.append(current.rstrip())
            current = block
        else:
            current += block
    if current:
        chunks.append(current.rstrip())
    return chunks


def send_telegram_message(token: str, chat_id: str, text: str):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, data={"chat_id": chat_id, "text": text}, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("ok"):
        raise RuntimeError(f"Telegram API error: {payload}")


def notify_new_ads(new_rows: list[dict], token: str, chat_id: str):
    if not new_rows:
        send_telegram_message(token, chat_id, "\u041d\u043e\u0432\u044b\u0445 \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u0439 \u043d\u0435\u0442. \u041d\u0438\u0447\u0435\u0433\u043e \u0441\u0442\u0440\u0430\u0448\u043d\u043e\u0433\u043e, \u0436\u0434\u0438\u0442\u0435 \u0437\u0430\u0432\u0442\u0440\u0430.")
        print("\u041d\u043e\u0432\u044b\u0445 \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u0439 \u043f\u043e\u0434 \u0444\u0438\u043b\u044c\u0442\u0440 \u043d\u0435\u0442.")
        return

    new_rows.sort(key=lambda r: r["price_int"])
    header = (
        "\u041d\u043e\u0432\u044b\u0435 \u043e\u0431\u044a\u044f\u0432\u043b\u0435\u043d\u0438\u044f Krisha\n"
        f"\u0424\u0438\u043b\u044c\u0442\u0440: {TARGET_ROOMS} \u043a\u043e\u043c\u043d\u0430\u0442\u044b, \u0446\u0435\u043d\u0430 <= {MAX_PRICE:,} \u0442\u0433".replace(",", " ")
        + f"\n\u041d\u0430\u0439\u0434\u0435\u043d\u043e \u043d\u043e\u0432\u044b\u0445: {len(new_rows)}"
    )
    send_telegram_message(token, chat_id, header)

    lines = []
    for idx, row in enumerate(new_rows, start=1):
        lines.append(
            f"{idx}) {row[COL_TITLE]}\n"
            f"\u0426\u0435\u043d\u0430: {row[COL_PRICE]}\n"
            f"\u0421\u0441\u044b\u043b\u043a\u0430: {row[COL_LINK]}"
        )

    for chunk in split_messages(lines):
        send_telegram_message(token, chat_id, chunk)


def run(max_pages: int | None = None):
    token = TELEGRAM_BOT_TOKEN or ""
    chat_id = TELEGRAM_CHAT_ID or ""
    if not token or not chat_id:
        raise RuntimeError(
            "\u041d\u0435 \u0437\u0430\u0434\u0430\u043d\u044b TELEGRAM_BOT_TOKEN \u0438/\u0438\u043b\u0438 TELEGRAM_CHAT_ID. "
            "\u0417\u0430\u043f\u043e\u043b\u043d\u0438 \u0438\u0445 \u0432 \u043a\u043e\u0434\u0435 \u0438\u043b\u0438 \u043f\u0435\u0440\u0435\u0434\u0430\u0439 \u0447\u0435\u0440\u0435\u0437 \u043f\u0435\u0440\u0435\u043c\u0435\u043d\u043d\u044b\u0435 \u043e\u043a\u0440\u0443\u0436\u0435\u043d\u0438\u044f."
        )

    rows = scrape_all(max_pages=max_pages)
    filtered = filter_target(rows)

    seen_ids = load_seen_ids(SEEN_IDS_FILE)
    new_rows = [row for row in filtered if row["ad_id"] not in seen_ids]

    notify_new_ads(new_rows, token=token, chat_id=chat_id)

    if new_rows:
        seen_ids.update(row["ad_id"] for row in new_rows)
        save_seen_ids(SEEN_IDS_FILE, seen_ids)
        print(f"\u0421\u043e\u0445\u0440\u0430\u043d\u0435\u043d\u043e \u043d\u043e\u0432\u044b\u0445 ID: {len(new_rows)}")
    else:
        print("\u0421\u043e\u0441\u0442\u043e\u044f\u043d\u0438\u0435 \u0431\u0435\u0437 \u0438\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0439.")


def load_env_overrides():
    import os

    global TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, MAX_PAGES

    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN)
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", TELEGRAM_CHAT_ID)

    max_pages_raw = os.getenv("MAX_PAGES", "").strip()
    if max_pages_raw:
        MAX_PAGES = int(max_pages_raw)


if __name__ == "__main__":
    load_env_overrides()
    run(max_pages=MAX_PAGES)
