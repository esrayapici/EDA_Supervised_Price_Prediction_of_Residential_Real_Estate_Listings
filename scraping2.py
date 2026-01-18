import os
import re
import time
import random
import csv
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# CONFIG
BASE_URL = "https://www.storia.ro"
START_URL_TEMPLATE = "https://www.storia.ro/ro/rezultate/vanzare/apartament/bucuresti?limit=72&page={page}"

MAX_PAGES = 20
OUTPUT_CSV = "storia_real_estate_dataset_3.csv"
DEBUG_DIR = "debug_pages"

HEADLESS = True
GOTO_TIMEOUT_MS = 45_000

MIN_SLEEP = 1.5
MAX_SLEEP = 3.5

ENABLE_DETAIL_ENRICH = True
DETAIL_SLEEP_MIN = 0.8
DETAIL_SLEEP_MAX = 1.6
DETAIL_TIMEOUT_MS = 35_000

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]


def clean_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s or None

def parse_price_raw(s: Optional[str]) -> Optional[str]:
    return clean_text(s)

def listing_id_from_link(link: Optional[str]) -> Optional[str]:
    """Try to derive a stable id from URL."""
    if not link:
        return None
    m = re.search(r"-(\d+)\.html", link)
    if m:
        return m.group(1)
    m2 = re.search(r"[?&]id=(\d+)", link)
    if m2:
        return m2.group(1)
    return link.rstrip("/").split("/")[-1][:80]

def extract_sector(location_info: Optional[str]) -> Optional[str]:
    if not location_info:
        return None
    m = re.search(r"sector(?:ul)?\s*(\d)", location_info.lower())
    return m.group(1) if m else None

def pick_first_attr(el, attr_names: List[str]) -> Optional[str]:
    if not el:
        return None
    for a in attr_names:
        v = clean_text(el.get_attribute(a))
        if v:
            return v
    return None

def looks_like_listing_link(href: Optional[str]) -> bool:
    if not href:
        return False
    return ("/oferta/" in href) or ("/oferta" in href)

def normalize_link(href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith("/"):
        return BASE_URL + href
    if href.startswith("http"):
        return href
    return None

def safe_int_from_match(text: str, pattern: str) -> Optional[int]:
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def accept_cookies(page) -> None:
    try:
        cookie_btn = page.query_selector("button#onetrust-accept-btn-handler")
        if cookie_btn:
            cookie_btn.click()
            page.wait_for_timeout(400)
    except Exception:
        pass

# Extraction logic (PAGE LISTING)

TITLE_SELECTORS = [
    "[data-cy*='title']",
    "[data-testid*='title']",
    "h2",
    "h3",
    "a[title]",
    "a[aria-label]",
]

LOCATION_SELECTORS = [
    "[data-cy*='location']",
    "[data-testid*='location']",
    "[data-cy*='address']",
    "[data-testid*='address']",
    "p:has-text('Sector')",
]

PRICE_SELECTORS = [
    "[data-cy*='price']",
    "[data-testid*='price']",
    "p:has-text('€')",
    "span:has-text('€')",
]

def extract_title(card) -> Optional[str]:
    for sel in TITLE_SELECTORS:
        el = card.query_selector(sel)
        if el:
            t = clean_text(el.inner_text())
            if t and len(t) >= 3:
                return t
    a = card.query_selector("a[href]")
    if a:
        t = pick_first_attr(a, ["title", "aria-label"])
        if t and len(t) >= 3:
            return t
    return None

def extract_listing_link(card) -> Optional[str]:
    anchors = card.query_selector_all("a[href]")
    for a in anchors:
        href = a.get_attribute("href")
        if looks_like_listing_link(href):
            return normalize_link(href)
    a = card.query_selector("a[href]")
    return normalize_link(a.get_attribute("href")) if a else None

def extract_location(card) -> Optional[str]:
    for sel in LOCATION_SELECTORS:
        el = card.query_selector(sel)
        if el:
            t = clean_text(el.inner_text())
            if t and len(t) >= 2:
                return t
    return None

def extract_price(card) -> Optional[str]:
    for sel in PRICE_SELECTORS:
        el = card.query_selector(sel)
        if el:
            t = parse_price_raw(el.inner_text())
            if t and len(t) >= 1:
                return t
    return None

def parse_rooms_area_from_text(card_text: str) -> Tuple[Optional[int], Optional[float]]:
    rooms = None
    area = None

    m_rooms = re.search(r"(\d+)\s*(camere|camera)", card_text.lower())
    if m_rooms:
        rooms = int(m_rooms.group(1))

    m_area = re.search(r"(\d+([.,]\d+)?)\s*m²", card_text.lower())
    if m_area:
        area = float(m_area.group(1).replace(",", "."))

    return rooms, area

def get_listing_cards(page):
    cards = page.query_selector_all("article:has(a[href*='/oferta'])")
    if not cards:
        cards = page.query_selector_all("[data-cy*='listing']:has(a[href]), [data-testid*='listing']:has(a[href])")
    if not cards:
        cards = page.query_selector_all("article")
    return cards

# DETAIL PAGE ENRICHMENT  for missing fields

DETAIL_LOCATION_SELECTORS = [
    "[data-cy*='address']",
    "[data-testid*='address']",
    "[data-cy*='location']",
    "[data-testid*='location']",
    "header *:has-text('Sector')",
]

DETAIL_ROOMS_SELECTORS = [
    "[data-cy*='rooms']",
    "[data-testid*='rooms']",
    "li:has-text('camere')",
    "span:has-text('camere')",
]

def extract_location_from_detail(detail_page) -> Optional[str]:
    for sel in DETAIL_LOCATION_SELECTORS:
        el = detail_page.query_selector(sel)
        if el:
            t = clean_text(el.inner_text())
            if t and len(t) >= 2:
                return t
    body_text = clean_text(detail_page.inner_text("body")) or ""

    m = re.search(r"(București[^.\n]{0,120}sector(?:ul)?\s*\d)", body_text, flags=re.IGNORECASE)
    if m:
        return clean_text(m.group(1))
    return None

def extract_rooms_from_detail(detail_page) -> Optional[int]:
    # Try structured selectors first
    for sel in DETAIL_ROOMS_SELECTORS:
        el = detail_page.query_selector(sel)
        if el:
            t = clean_text(el.inner_text()) or ""
            r = safe_int_from_match(t, r"(\d+)\s*(camere|camera)")
            if r is not None:
                return r
    # fallback: scan page text
    body_text = clean_text(detail_page.inner_text("body")) or ""
    r = safe_int_from_match(body_text, r"(\d+)\s*(camere|camera)")
    return r

def enrich_from_detail(detail_page, link: str, need_location: bool, need_rooms: bool) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {}
    try:
        detail_page.goto(link, wait_until="domcontentloaded", timeout=DETAIL_TIMEOUT_MS)
        detail_page.wait_for_timeout(900)
        accept_cookies(detail_page)

        if need_location:
            loc = extract_location_from_detail(detail_page)
            out["Location_Info"] = loc
            out["Sector"] = extract_sector(loc)

        if need_rooms:
            rooms = extract_rooms_from_detail(detail_page)
            out["Room_Number"] = rooms

        time.sleep(random.uniform(DETAIL_SLEEP_MIN, DETAIL_SLEEP_MAX))
        return out

    except Exception:
        return out

# Page scraping

def scrape_page(page, detail_page, page_num: int, detail_cache: Dict[str, Dict]) -> List[Dict]:
    url = START_URL_TEMPLATE.format(page=page_num)
    print(f"\n[OPEN] Page {page_num}: {url}")

    page.goto(url, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
    page.wait_for_timeout(1500)

    accept_cookies(page)

    # small scroll to trigger lazy load
    page.mouse.wheel(0, 1400)
    page.wait_for_timeout(900)

    try:
        page.wait_for_selector("a[href]", timeout=12_000)
    except PlaywrightTimeoutError:
        pass

    cards = get_listing_cards(page)

    if not cards:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        html = page.content()
        with open(os.path.join(DEBUG_DIR, f"page_{page_num}.html"), "w", encoding="utf-8") as f:
            f.write(html)
        return []

    rows = []
    for c in cards:
        link = extract_listing_link(c)
        if not link or ("/oferta" not in link):
            continue

        title = extract_title(c)
        price_raw = extract_price(c)
        location_info = extract_location(c)

        card_text = clean_text(c.inner_text()) or ""
        rooms, area = parse_rooms_area_from_text(card_text)

        sector = extract_sector(location_info)
        lid = listing_id_from_link(link)

        # enrich only needed
        if ENABLE_DETAIL_ENRICH:
            need_location = (location_info is None) or (sector is None)
            need_rooms = (rooms is None)

            if (need_location or need_rooms) and lid:
                if lid in detail_cache:
                    cached = detail_cache[lid]
                else:
                    cached = enrich_from_detail(
                        detail_page=detail_page,
                        link=link,
                        need_location=need_location,
                        need_rooms=need_rooms,
                    )
                    detail_cache[lid] = cached

                if need_location:
                    location_info = location_info or cached.get("Location_Info")
                    sector = sector or cached.get("Sector")

                if need_rooms:
                    rooms = rooms if rooms is not None else cached.get("Room_Number")

        row = {
            "listing_id": lid,
            "Property_Title": title,
            "Price_Raw": price_raw,
            "Location_Info": location_info,
            "Sector": sector,
            "Room_Number": rooms,
            "Area_m2": area,
            "Link": link,
            "Scraped_At": datetime.now().isoformat(timespec="seconds"),
            "Page": page_num,
        }

        if row["Link"] and (row["Property_Title"] or row["Price_Raw"] or row["Location_Info"]):
            rows.append(row)

    if len(rows) < 5:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        html = page.content()
        with open(os.path.join(DEBUG_DIR, f"page_{page_num}_lowrows.html"), "w", encoding="utf-8") as f:
            f.write(html)

    return rows

def init_csv(path: str):
    header = [
        "listing_id",
        "Property_Title",
        "Price_Raw",
        "Location_Info",
        "Sector",
        "Room_Number",
        "Area_m2",
        "Link",
        "Scraped_At",
        "Page",
    ]
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)

def append_rows(path: str, rows: List[Dict]):
    if not rows:
        return 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        for r in rows:
            writer.writerow(r)
    return len(rows)

def run():
    os.makedirs(DEBUG_DIR, exist_ok=True)
    init_csv(OUTPUT_CSV)

    total_rows = 0
    seen_ids = set()
    detail_cache: Dict[str, Dict] = {}  # listing_id -> enriched values

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1366, "height": 768},
            locale="ro-RO",
        )

        page = context.new_page()
        detail_page = context.new_page()  # separate tab for detail enrichment

        for page_num in range(1, MAX_PAGES + 1):
            page_rows = []
            for attempt in range(1, 4):
                try:
                    page_rows = scrape_page(page, detail_page, page_num, detail_cache)
                    break
                except PlaywrightTimeoutError:
                    wait = 2.0 * attempt + random.random()
                    print(f"[WARN] Timeout on page {page_num}, attempt {attempt}. Sleep {wait:.1f}s and retry...")
                    time.sleep(wait)
                except Exception as e:
                    wait = 2.0 * attempt + random.random()
                    print(f"[WARN] Error on page {page_num}, attempt {attempt}: {type(e).__name__} | Sleep {wait:.1f}s and retry...")
                    time.sleep(wait)

            # Dedup in-run using listing_id
            unique_rows = []
            for r in page_rows:
                lid = r.get("listing_id")
                if lid and lid in seen_ids:
                    continue
                if lid:
                    seen_ids.add(lid)
                unique_rows.append(r)

            added = append_rows(OUTPUT_CSV, unique_rows)
            total_rows += added
            print(f"[SAVE] Page {page_num}: {added} new rows | Total: {total_rows}")

            time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))

        browser.close()

    print("\n[DONE] Final rows saved:", total_rows)
    print("[DONE] Output file:", OUTPUT_CSV)
    print("[DONE] Debug HTML folder:", DEBUG_DIR)
    print("[DONE] Detail cache size:", len(detail_cache))

if __name__ == "__main__":
    run()
