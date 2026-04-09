# -*- coding: utf-8 -*-
"""
CRAWLER CHI SO THI TRUONG BDS - batdongsan.com.vn
==================================================
Thu thap tu dong 5 chi so du doan thi truong cho 15 khu vuc Ha Noi:
  1. Ty le tin "cat lo / ban gap" (Distress Ratio)
  2. % Dang hom nay
  3. Gia ban trung binh / m2
  4. Gia thue trung binh / thang
  5. Rental Yield uoc tinh

Che do: Crawl TOAN BO cac trang (khong gioi han)

Cach chay:
  pip install curl_cffi beautifulsoup4
  python crawl_chi_so_thi_truong.py
"""

import sys
import io
import csv
import re
import time
import json
import random
import argparse
import math
from datetime import datetime, timedelta
from pathlib import Path

# Defensive utils (DC01-DC07)
try:
    from defensive_utils import (
        safe_append_csv, backup_csv, log_warning, log_error, log_success,
        save_progress, load_progress, clear_progress,
    )
    HAS_DEFENSIVE = True
except ImportError:
    HAS_DEFENSIVE = False

# Fix Windows console encoding
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from curl_cffi import requests
from bs4 import BeautifulSoup

# ============================================================
# CONFIGURATION
# ============================================================

KHU_VUC = {
    # Top 16 quận/huyện có lượng tin đăng cao nhất
    "Nam Từ Liêm":   "nam-tu-liem",       # 2,276 tin
    "Cầu Giấy":      "cau-giay",          # 1,855 tin
    "Hà Đông":       "ha-dong",           # 1,781 tin
    "Hoàng Mai":     "hoang-mai",          # 1,436 tin
    "Thanh Xuân":    "thanh-xuan",         # 1,362 tin
    "Tây Hồ":        "tay-ho",            #   964 tin
    "Bắc Từ Liêm":  "bac-tu-liem",        #   856 tin
    "Gia Lâm":       "gia-lam",           #   712 tin
    "Long Biên":     "long-bien",          #   650 tin
    "Hoài Đức":      "hoai-duc",          #   557 tin
    "Đống Đa":       "dong-da",           #   521 tin
    "Hai Bà Trưng":  "hai-ba-trung",      #   432 tin
    "Thanh Trì":     "thanh-tri",
    "Ba Đình":       "ba-dinh",           #   259 tin
    "Đông Anh":      "dong-anh",          #   176 tin
    "Đan Phượng":    "dan-phuong",        #    68 tin
}

URL_BAN      = "https://batdongsan.com.vn/ban-nha-rieng-{slug}"
URL_CHUNG_CU = "https://batdongsan.com.vn/ban-can-ho-chung-cu-{slug}"
URL_BIET_THU = "https://batdongsan.com.vn/ban-biet-thu-lien-ke-{slug}"

MAX_PAGES = 1    # Chi crawl trang 1 (nhanh, an toan)
BATCH_SIZE = 4   # So khu vuc moi lan chay (khi dung --batch)

DISTRESS_KEYWORDS = [
    "cắt lỗ", "cat lo", "bán gấp", "ban gap",
    "bán lỗ", "ban lo", "giảm giá", "giam gia",
    "ngộp", "ngop", "bán cắt lỗ", "giá rẻ",
    "cần bán gấp", "cần tiền bán gấp", "chính chủ cần bán gấp",
]

# Danh sach User-Agent xoay vong (cho Playwright)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

import os
OUTPUT_DIR = Path(os.environ["BDS_DATA_DIR"]) if "BDS_DATA_DIR" in os.environ else Path(__file__).parent


# ============================================================
# PARSER
# ============================================================

def parse_price_text(text: str) -> float | None:
    if not text:
        return None
    text = text.strip().lower().replace(",", ".")
    m = re.search(r"([\d.]+)\s*tỷ", text)
    if m:
        return float(m.group(1)) * 1000
    m = re.search(r"([\d.]+)\s*triệu", text)
    if m:
        return float(m.group(1))
    return None


def parse_price_per_m2(text: str) -> float | None:
    if not text:
        return None
    text = text.strip().lower().replace(",", ".")
    m = re.search(r"([\d.]+)\s*tr", text)
    if m:
        return float(m.group(1))
    return None


def parse_area(text: str) -> float | None:
    if not text:
        return None
    m = re.search(r"([\d.,]+)", text.replace(",", "."))
    if m:
        return float(m.group(1))
    return None


def parse_post_date(text: str) -> datetime | None:
    if not text:
        return None
    text = text.strip().lower()

    if "hôm nay" in text:
        return datetime.now()
    if "hôm qua" in text:
        return datetime.now() - timedelta(days=1)

    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass

    m = re.search(r"(\d+)\s*ngày", text)
    if m:
        return datetime.now() - timedelta(days=int(m.group(1)))

    m = re.search(r"(\d+)\s*tuần", text)
    if m:
        return datetime.now() - timedelta(weeks=int(m.group(1)))

    return None


def is_distress(title: str) -> bool:
    title_lower = title.lower()
    return any(kw in title_lower for kw in DISTRESS_KEYWORDS)


def extract_total_count(html: str) -> int | None:
    """Trich xuat tong so tin tu JS data trong HTML."""
    # Pattern 1: listing_count': 1439 (trong JS object)
    m = re.search(r"listing_count'?\s*:\s*(\d+)", html)
    if m:
        count = int(m.group(1))
        if count > 0:
            return count
    # Pattern 2: "totalCount": 1439
    m = re.search(r'"totalCount"\s*:\s*(\d+)', html)
    if m:
        return int(m.group(1))
    # Pattern 3: Hiện có X bất động sản
    m = re.search(r'Hiện có\s*([\d.]+)\s*bất', html)
    if m:
        return int(m.group(1).replace(".", ""))
    return None


def extract_total_views(html: str) -> int | None:
    """Trich xuat tong luot xem khu vuc tu HTML (re__srp-traffic-label)."""
    # Pattern 1: HTML entity encoded: "C&#xF3; 2.782 l&#x1B0;&#x1EE3;t xem"
    m = re.search(r'srp-traffic-label[^>]*>[^<]*?([\d.,]+)\s*(?:l[^t]*t|&#x1B0;)', html)
    if m:
        return int(m.group(1).replace(".", "").replace(",", ""))
    # Pattern 2: Plain text: "Có 2.782 lượt xem"
    m = re.search(r'Có\s*([\d.,]+)\s*lượt\s*xem', html)
    if m:
        return int(m.group(1).replace(".", "").replace(",", ""))
    # Pattern 3: generic number near "lượt xem" / "luot xem"
    m = re.search(r'([\d.,]+)\s*(?:lượt|luot)\s*xem', html, re.IGNORECASE)
    if m:
        return int(m.group(1).replace(".", "").replace(",", ""))
    return None


def extract_price_yoy(html: str) -> float | None:
    """Trich xuat % tang gia YoY tu trang search (entry-number / entry-text)."""
    # Pattern: <div class="entry-number">35%</div> ... Giá bán đã tăng
    m = re.search(
        r'entry-number[^>]*>\s*(\d+)%\s*</div>\s*<div[^>]*entry-text[^>]*>.*?[Gg]iá\s*bán.*?tăng',
        html, re.DOTALL
    )
    if m:
        return float(m.group(1))
    # Pattern 2: down-trend
    m = re.search(
        r'entry-number[^>]*>\s*(\d+)%\s*</div>\s*<div[^>]*entry-text[^>]*>.*?[Gg]iá\s*bán.*?giảm',
        html, re.DOTALL
    )
    if m:
        return -float(m.group(1))
    return None


def extract_listings(html: str) -> list[dict]:
    """Trich xuat danh sach tin dang tu HTML."""
    soup = BeautifulSoup(html, "html.parser")
    listings = []

    # DC02: Nhiều selector fallback
    CARD_SELECTORS = [
        ".re__card-full-v3, .js__card",   # Selector hiện tại
        ".re__card-info",                  # Backup 1
        "[data-listing-id]",               # Backup 2
        ".product-listing__item",           # Backup 3
    ]
    cards = []
    for sel in CARD_SELECTORS:
        cards = soup.select(sel)
        if len(cards) >= 3:
            break
    if not cards and HAS_DEFENSIVE:
        log_warning("Không tìm thấy listings với bất kỳ selector nào")

    for card in cards:
        title_el = card.select_one(".re__card-title span, .re__card-title")
        title = title_el.get_text(strip=True) if title_el else ""
        if not title:
            continue

        link_el = card.select_one(
            "a.js__product-link-for-product-id, a[href]"
        )
        link = link_el.get("href", "") if link_el else ""
        if link and not link.startswith("http"):
            link = "https://batdongsan.com.vn" + link

        price_el = card.select_one(".re__card-config-price")
        price_text = price_el.get_text(strip=True) if price_el else ""
        price = parse_price_text(price_text)

        area_el = card.select_one(".re__card-config-area")
        area_text = area_el.get_text(strip=True) if area_el else ""
        area = parse_area(area_text)

        ppm2_el = card.select_one(".re__card-config-price-per-m2")
        ppm2_text = ppm2_el.get_text(strip=True) if ppm2_el else ""
        price_per_m2 = parse_price_per_m2(ppm2_text)

        if not price_per_m2 and price and area and area > 0:
            price_per_m2 = price / area

        loc_el = card.select_one(".re__card-location")
        location = loc_el.get_text(strip=True) if loc_el else ""

        date_el = card.select_one(
            ".re__card-published-info-published-at, "
            ".re__card-published-info span"
        )
        date_text = date_el.get_text(strip=True) if date_el else ""
        post_date = parse_post_date(date_text)

        listings.append({
            "title": title,
            "link": link,
            "price": price,
            "area": area,
            "price_per_m2": price_per_m2,
            "location": location,
            "post_date": post_date,
            "post_date_text": date_text,
            "is_distress": is_distress(title),
        })

    return listings


# ============================================================
# CRAWLER - dung Playwright (headless browser) bypass Cloudflare
# ============================================================

MAX_RETRIES = 3  # So lan thu lai khi gap loi
DELAY_BETWEEN_PAGES = (2, 4)    # Delay giua cac trang (giay)
DELAY_BETWEEN_REGIONS = (5, 10) # Delay giua cac khu vuc (giay)
CLOUDFLARE_WAIT = 15  # So giay doi Cloudflare challenge tu giai
LISTING_SELECTORS = ".re__card-full-v3, .js__card, .re__card-info, .product-listing__item"


# Removed simulate_human_activity because curl_cffi handles bypass at the protocol level.


def fetch_page(url: str, session, retry: int = 0) -> str | None:
    """
    Tai 1 trang va tra ve HTML string bang curl_cffi.
    session o day la requests.Session(impersonate="chrome120").
    """
    try:
        # De curl_cffi tu quan ly headers va UA thong qua impersonate
        headers = {
            "referer": "https://batdongsan.com.vn/",
        }

        response = session.get(url, headers=headers, timeout=30)
        html = response.text
        status = response.status_code

        # Kiem tra tinh hop le cua du lieu (status != 200 hoac co text khoa chan)
        is_blocked = status != 200 or "Just a moment" in html
        is_empty = len(html) < 20000 
        
        if (is_blocked or is_empty) and retry < MAX_RETRIES:
            reason = f"Cloudflare chan (Status: {status})" if is_blocked else "du lieu trong"
            wait = random.uniform(15, 30)
            print(f"    !! {reason} - doi {wait:.0f}s roi thu lai ({retry+1}/{MAX_RETRIES})")
            time.sleep(wait)
            return fetch_page(url, session, retry + 1)
        elif is_blocked or is_empty:
            print(f"    !! {('Cloudflare van chan' if is_blocked else 'Du lieu van trong')} sau {MAX_RETRIES} lan thu - Bo qua")
            return None

        return html

    except Exception as e:
        if retry < MAX_RETRIES:
            wait = random.uniform(8, 15)
            print(f"    !! Error: {e} - doi {wait:.0f}s roi thu lai ({retry+1}/{MAX_RETRIES})")
            time.sleep(wait)
            return fetch_page(url, session, retry + 1)
        print(f"    !! Error (da thu {MAX_RETRIES} lan): {e}")
        return None


def crawl_listings(session, slug: str, listing_type: str = "ban",
                   max_pages: int = MAX_PAGES) -> tuple[list[dict], int | None, int | None, float | None]:
    """Crawl tin dang cho 1 khu vuc. Tra ve (listings, total_count, total_views, price_yoy)."""
    url_templates = {"ban": URL_BAN, "biet_thu": URL_BIET_THU}
    url_template = url_templates.get(listing_type, URL_BAN)
    base_url = url_template.format(slug=slug)
    all_listings = []
    total_count = None
    total_views = None
    price_yoy = None
    estimated_pages = "?"

    for page in range(1, max_pages + 1):
        url = base_url if page == 1 else f"{base_url}/p{page}"
        print(f"    Trang {page}/{estimated_pages}: {url}")

        html = fetch_page(url, session)
        if html is None:
            print(f"    -> Khong tai duoc trang - dung lai")
            break

        # Lay tong so tin + luot xem + % tang gia tu trang dau tien
        if page == 1:
            total_count = extract_total_count(html)
            total_views = extract_total_views(html)
            price_yoy = extract_price_yoy(html)
            if total_count:
                estimated_pages = (total_count + 19) // 20  # ~20 tin/trang
                print(f"    -> Tong tren web: {total_count:,} tin (~{estimated_pages} trang)")
            if total_views:
                print(f"    -> Luot xem khu vuc: {total_views:,}")
            if price_yoy is not None:
                print(f"    -> Gia ban tang/giam: {price_yoy:+.0f}% YoY")

        listings = extract_listings(html)
        if not listings:
            print(f"    -> Khong tim thay tin - dung lai (da crawl {len(all_listings)} tin)")
            break

        all_listings.extend(listings)
        print(f"    -> +{len(listings)} tin (tong: {len(all_listings)})")

        # Delay ngau nhien giua cac trang
        delay = random.uniform(*DELAY_BETWEEN_PAGES)
        time.sleep(delay)

    print(f"    => TONG: {len(all_listings)} tin crawl duoc")
    return all_listings, total_count, total_views, price_yoy


# ============================================================
# CALCULATOR
# ============================================================

def calculate_indicators(ban_listings: list[dict],
                         real_total_ban: int | None = None) -> dict:
    result = {
        "total_ban": real_total_ban or len(ban_listings),
        "crawled_ban": len(ban_listings),
        "distress_count": 0,
        "distress_ratio": None,
        "today_count": 0,
        "pct_today": None,
        "avg_price_per_m2": None,
    }

    if not ban_listings:
        return result

    total = real_total_ban or len(ban_listings)
    sample_size = len(ban_listings)

    # 1. Distress Ratio
    distress = [l for l in ban_listings if l["is_distress"]]
    result["distress_count"] = len(distress)
    result["distress_ratio"] = len(distress) / sample_size * 100 if sample_size else None

    # 2. Tin dang hom nay
    now = datetime.now()
    today_count = 0
    for l in ban_listings:
        if l["post_date"]:
            days = (now - l["post_date"]).days
            if days == 0:
                today_count += 1
    result["today_count"] = today_count
    result["pct_today"] = today_count / sample_size * 100 if sample_size else None

    # 3. Gia ban TB / m2 (MEDIAN - khang outlier tot nhat)
    all_prices = [l["price_per_m2"] for l in ban_listings if l["price_per_m2"]]
    prices_m2 = [p for p in all_prices if 10 < p < 500]

    # DC03: Cảnh báo nếu > 30% giá bị filter
    if all_prices and HAS_DEFENSIVE:
        filtered_count = len(all_prices) - len(prices_m2)
        filter_ratio = filtered_count / len(all_prices) * 100
        if filter_ratio > 30:
            log_warning(
                f"{filter_ratio:.0f}% giá bị loại ({filtered_count}/{len(all_prices)}) "
                f"— có thể website đổi format giá!"
            )

    if prices_m2:
        prices_m2.sort()
        n = len(prices_m2)
        mid = n // 2
        result["avg_price_per_m2"] = prices_m2[mid] if n % 2 == 1 else (prices_m2[mid-1] + prices_m2[mid]) / 2

    return result


# ============================================================
# OUTPUT
# ============================================================

def fmt(val, decimals=1, suffix=""):
    if val is None:
        return "N/A"
    return f"{val:,.{decimals}f}{suffix}"


def distress_rating(ratio):
    if ratio is None: return "N/A"
    if ratio > 20: return "🔴 Rất cao — thị trường yếu"
    if ratio > 10: return "🟠 Cao — áp lực bán"
    if ratio > 5:  return "🟡 Trung bình"
    return "🟢 Thấp — thị trường khỏe"


def dom_rating(pct):
    """Danh gia do moi tin dang (% dang hom nay)."""
    if pct is None: return "N/A"
    if pct > 60: return "🔴 Rất sôi động — tin đăng liên tục"
    if pct > 40: return "🟠 Khá sôi động"
    if pct > 20: return "🟡 Bình thường"
    return "🟢 Ít hoạt động"


def yield_rating(y):
    if y is None: return "N/A"
    if y > 5: return "🟢 Tốt — đầu tư cho thuê hiệu quả"
    if y > 3: return "🟡 Khá — chấp nhận được"
    return "🔴 Thấp — giá bán cao so với thuê"


def generate_markdown_report(all_results: dict, timestamp: str) -> str:
    lines = [
        "# 📊 CHỈ SỐ THỊ TRƯỜNG BĐS HÀ NỘI",
        "",
        f"> **Nguồn:** batdongsan.com.vn | **Ngày crawl:** {timestamp}",
        "> **Phương pháp:** Crawl trang 1 tự động — 15 quận/huyện",
        "",
        "---",
        "",
        "## I. BẢNG TỔNG HỢP",
        "",
        "| # | Khu vực | Tổng tin | Lượt xem | Giá TB (tr/m²) | % Tăng giá | % Cắt lỗ | Đăng hôm nay |",
        "|:-:|---------|:-------:|:-------:|:----------:|:---------:|:--------:|:----------:|",
    ]

    for i, (name, data) in enumerate(all_results.items(), 1):
        ind = data["indicators"]
        lines.append(
            f"| {i} | **{name}** "
            f"| {ind['total_ban']:,} "
            f"| {ind.get('views_ban') or 'N/A'} "
            f"| {fmt(ind['avg_price_per_m2'], 1)} "
            f"| {fmt(ind.get('price_yoy'), 0, '%')} "
            f"| {fmt(ind['distress_ratio'], 1, '%')} "
            f"| {ind['today_count']} |"
        )

    lines.extend(["", "---", ""])
    lines.extend([
        "> ⚠️ **Lưu ý:** Dữ liệu là **giá chào** (listing price), không phải giá giao dịch thực tế.",
        "",
        "---",
        "",
        f"📅 **Crawl lúc:** {timestamp} | 📁 **Nguồn:** batdongsan.com.vn | 🤖 **Script:** crawl_chi_so_thi_truong.py",
    ])

    return "\n".join(lines)


def save_csv(all_results: dict, filepath: Path):
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Khu vực", "Tiêu đề", "Giá (triệu)",
            "Diện tích (m²)", "Giá/m² (triệu)", "Vị trí",
            "Ngày đăng", "Cắt lỗ?", "Link"
        ])
        for name, data in all_results.items():
            for listing in data.get("ban_listings", []):
                writer.writerow([
                    name, listing["title"],
                    listing["price"], listing["area"],
                    listing["price_per_m2"], listing["location"],
                    listing["post_date_text"],
                    "Có" if listing["is_distress"] else "",
                    listing["link"],
                ])


def save_history(all_results: dict, filepath: Path):
    """Luu lich su chi so theo ngay — append, khong ghi de."""
    headers = [
        "Ngày", "Khu vực",
        "Tổng tin bán", "Lượt xem khu vực",
        "Tin cắt lỗ", "% Cắt lỗ",
        "Đăng hôm nay", "% Hôm nay",
        "Giá bán TB (tr/m²)",
        "Giá chung cư (tr/m²)",
        "% Tăng giá YoY",
    ]
    today = datetime.now().strftime("%Y-%m-%d")

    # Chuẩn bị rows
    rows = []
    for name, data in all_results.items():
        ind = data["indicators"]
        rows.append([
            today, name,
            ind["total_ban"],
            ind.get("views_ban", ""),
            ind["distress_count"],
            round(ind["distress_ratio"], 2) if ind["distress_ratio"] else "",
            ind["today_count"],
            round(ind["pct_today"], 2) if ind["pct_today"] else "",
            round(ind["avg_price_per_m2"], 1) if ind["avg_price_per_m2"] else "",
            ind.get("gia_chung_cu", ""),
            ind.get("price_yoy", ""),
        ])

    # DC01: Chống ghi trùng
    if HAS_DEFENSIVE:
        safe_append_csv(filepath, rows, headers, date_column="Ngày")
    else:
        file_exists = filepath.exists()
        with open(filepath, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(headers)
            writer.writerows(rows)


# ============================================================
# MAIN
# ============================================================

def run_crawl(regions: dict, session) -> dict:
    """Crawl trang 1 BAN cho moi khu vuc. Tra ve dict ket qua."""
    all_results = {}
    total_regions = len(regions)
    progress_file = OUTPUT_DIR / "_crawl_progress.json"

    # DC05: Resume từ checkpoint nếu có
    done_slugs = set()
    if HAS_DEFENSIVE:
        progress = load_progress(progress_file)
        if progress:
            done_slugs = set(progress.get("done_regions", []))
            if done_slugs:
                print(f"🔄 Resume từ checkpoint: đã xong {len(done_slugs)}/{total_regions} KV")

    for idx, (name, slug) in enumerate(regions.items(), 1):
        # DC05: Skip khu vực đã crawl
        if slug in done_slugs:
            print(f"  ⏭️  Skip {name} (đã crawl từ checkpoint)")
            continue
        print(f"\n{'=' * 50}")
        print(f"[{idx}/{total_regions}] {name} (slug: {slug})")
        print(f"{'=' * 50}")

        ban, total_ban, views_ban, price_yoy = crawl_listings(session, slug, "ban", MAX_PAGES)
        print(f"  => Tin: {total_ban or 'N/A'} | Views: {views_ban or 'N/A'} | YoY: {price_yoy or 'N/A'}%")

        indicators = calculate_indicators(ban, total_ban)
        indicators["views_ban"] = views_ban
        indicators["price_yoy"] = price_yoy

        # Crawl thêm giá chung cư
        gia_chung_cu = None
        try:
            cc_url = URL_CHUNG_CU.format(slug=slug)
            print(f"  >> Crawl gia chung cu: {cc_url}")
            pause = random.uniform(2, 4)
            time.sleep(pause)
            cc_html = fetch_page(cc_url, session)
            if cc_html:
                cc_listings = extract_listings(cc_html)
                cc_prices = [l["price_per_m2"] for l in cc_listings if l.get("price_per_m2") and 10 < l["price_per_m2"] < 500]
                if cc_prices:
                    cc_prices.sort()
                    n = len(cc_prices)
                    mid = n // 2
                    gia_chung_cu = cc_prices[mid] if n % 2 == 1 else (cc_prices[mid-1] + cc_prices[mid]) / 2
                    print(f"  >> Gia chung cu: {gia_chung_cu:.1f} tr/m2 ({len(cc_prices)} tin)")
                else:
                    print(f"  >> Khong co du lieu gia chung cu")
        except Exception as e:
            print(f"  >> Loi crawl chung cu: {e}")

        indicators["gia_chung_cu"] = round(gia_chung_cu, 1) if gia_chung_cu else None

        all_results[name] = {
            "indicators": indicators,
            "ban_listings": ban,
        }

        print(f"  >> Gia: {fmt(indicators['avg_price_per_m2'], 1)} tr/m2 "
              f"| Cat lo: {fmt(indicators['distress_ratio'], 1, '%')} "
              f"| Hom nay: {indicators.get('today_count', 0)} tin")

        # DC05: Save checkpoint sau mỗi khu vực
        if HAS_DEFENSIVE:
            save_progress(progress_file, all_results, done_slugs | {slug})

        # Delay ngan giua cac khu vuc
        if idx < total_regions:
            pause = random.uniform(*DELAY_BETWEEN_REGIONS)
            print(f"  ... Nghi {pause:.0f}s ...")
            time.sleep(pause)

    # DC05: Xóa checkpoint khi hoàn thành
    if HAS_DEFENSIVE:
        clear_progress(progress_file)

    return all_results


def run_crawl_biet_thu(regions: dict, session) -> dict:
    """Crawl trang 1 BIET THU cho moi khu vuc (tach rieng voi chung cu)."""
    all_results = {}
    total_regions = len(regions)

    print(f"  Loai BDS: BIET THU / LIEN KE\n")

    for idx, (name, slug) in enumerate(regions.items(), 1):
        print(f"\n{'=' * 50}")
        print(f"[BT {idx}/{total_regions}] {name} (slug: {slug})")
        print(f"{'=' * 50}")

        ban, total_ban, views_ban, price_yoy = crawl_listings(
            session, slug, "biet_thu", MAX_PAGES
        )
        print(f"  => Tin: {total_ban or 'N/A'} | Views: {views_ban or 'N/A'} | YoY: {price_yoy or 'N/A'}%")

        if not ban and total_ban is None:
            print(f"  => Khong co du lieu biet thu cho {name} — bo qua")
            continue

        indicators = calculate_indicators(ban, total_ban)
        indicators["views_ban"] = views_ban
        indicators["price_yoy"] = price_yoy
        all_results[name] = {
            "indicators": indicators,
            "ban_listings": ban,
        }

        print(f"  >> Gia: {fmt(indicators['avg_price_per_m2'], 1)} tr/m2 "
              f"| Cat lo: {fmt(indicators['distress_ratio'], 1, '%')} "
              f"| Hom nay: {indicators.get('today_count', 0)} tin")

        if idx < total_regions:
            pause = random.uniform(*DELAY_BETWEEN_REGIONS)
            print(f"  ... Nghi {pause:.0f}s ...")
            time.sleep(pause)

    return all_results


def save_partial(results: dict, batch_num: int):
    """Luu ket qua tam cua 1 batch ra file JSON."""
    # Chuyen doi data de JSON serializable
    serializable = {}
    for name, data in results.items():
        ind = data["indicators"].copy()
        serializable[name] = {"indicators": ind}
        # Luu listings rieng CSV
        for listing in data.get("ban_listings", []):
            if listing.get("post_date"):
                listing["post_date"] = listing["post_date"].isoformat()
        for listing in data.get("thue_listings", []):
            if listing.get("post_date"):
                listing["post_date"] = listing["post_date"].isoformat()
        serializable[name]["ban_listings"] = data.get("ban_listings", [])
        serializable[name]["thue_listings"] = data.get("thue_listings", [])

    path = OUTPUT_DIR / f"_partial_batch_{batch_num}.json"
    path.write_text(json.dumps(serializable, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    print(f"  -> Luu tam: {path}")


def load_partial(batch_num: int) -> dict | None:
    """Doc ket qua tam cua 1 batch."""
    path = OUTPUT_DIR / f"_partial_batch_{batch_num}.json"
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    # Chuyen post_date string -> datetime
    for name, region_data in data.items():
        for listing in region_data.get("ban_listings", []):
            if listing.get("post_date"):
                try:
                    listing["post_date"] = datetime.fromisoformat(listing["post_date"])
                except (ValueError, TypeError):
                    listing["post_date"] = None
        for listing in region_data.get("thue_listings", []):
            if listing.get("post_date"):
                try:
                    listing["post_date"] = datetime.fromisoformat(listing["post_date"])
                except (ValueError, TypeError):
                    listing["post_date"] = None
    return data


def output_results(all_results: dict, timestamp: str):
    """Xuat ket qua ra file."""
    print(f"\n{'=' * 60}")
    print("XUAT KET QUA")

    md_path = OUTPUT_DIR / "KET_QUA_CHI_SO_THI_TRUONG.md"
    report = generate_markdown_report(all_results, timestamp)
    md_path.write_text(report, encoding="utf-8")
    print(f"  -> Markdown: {md_path}")

    csv_path = OUTPUT_DIR / "du_lieu_tho_crawl.csv"
    save_csv(all_results, csv_path)
    print(f"  -> CSV: {csv_path}")

    history_path = OUTPUT_DIR / "lich_su_chi_so.csv"
    save_history(all_results, history_path)
    print(f"  -> Lich su: {history_path}")

    total_ban = sum(r['indicators']['total_ban'] for r in all_results.values())
    print(f"\nHOAN THANH! {total_ban} tin ban tu {len(all_results)} khu vuc.")


def main():
    parser = argparse.ArgumentParser(description="Crawler chi so thi truong BDS")
    parser.add_argument(
        "--batch", type=int, default=0, metavar="N",
        help=f"Chi crawl batch thu N (1, 2, 3...). "
             f"Moi batch = {BATCH_SIZE} khu vuc. 0 = crawl tat ca."
    )
    parser.add_argument(
        "--merge", action="store_true",
        help="Gop ket qua tu cac batch da chay va xuat bao cao."
    )
    parser.add_argument(
        "--biet-thu", action="store_true", dest="biet_thu",
        help="Crawl them biet thu / lien ke (tach rieng voi chung cu)."
    )
    args = parser.parse_args()

    print("=" * 60)
    print("CRAWLER CHI SO THI TRUONG BDS - batdongsan.com.vn")
    print("=" * 60)

    region_names = list(KHU_VUC.keys())
    total_batches = math.ceil(len(region_names) / BATCH_SIZE)
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M")

    # --- Che do MERGE ---
    if args.merge:
        print(f"\nGOP KET QUA tu {total_batches} batch...")
        all_results = {}
        for b in range(1, total_batches + 1):
            partial = load_partial(b)
            if partial:
                all_results.update(partial)
                print(f"  Batch {b}: {len(partial)} khu vuc")
            else:
                print(f"  !! Batch {b}: CHUA CHAY (thieu file)")
        if all_results:
            output_results(all_results, timestamp)
        else:
            print("\n!! Khong co du lieu de gop.")
        return

    # Tao Session curl_cffi bypass Cloudflare
    session = requests.Session(impersonate="chrome120")
    print("\nEngine: curl_cffi (Impersonate Chrome 120) - TLS/JA3 Bypass ON")

    # --- Che do BATCH ---
    if args.batch > 0:
        batch_num = args.batch
        start = (batch_num - 1) * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(region_names))
        batch_names = region_names[start:end]

        if not batch_names:
            print(f"\n!! Batch {batch_num} khong hop le. "
                  f"Chi co {total_batches} batch.")
            return

        batch_regions = {n: KHU_VUC[n] for n in batch_names}
        print(f"\nBATCH {batch_num}/{total_batches}: "
              f"{', '.join(batch_names)}")
        print(f"({len(batch_names)} khu vuc)\n")

        results = run_crawl(batch_regions, session)
        save_partial(results, batch_num)

        print(f"\n--- Batch {batch_num} hoan thanh! ---")
        if batch_num < total_batches:
            print(f"Chay tiep: python crawl_chi_so_thi_truong.py --batch {batch_num + 1}")
        print(f"Gop & xuat: python crawl_chi_so_thi_truong.py --merge")
        return

    # --- Che do FULL (mac dinh) ---
    print(f"\nCHE DO FULL: crawl tat ca {len(KHU_VUC)} khu vuc")
    print(f"(Dung --batch N de chia nho)\n")

    all_results = run_crawl(KHU_VUC, session)
    output_results(all_results, timestamp)

    # Dong Session
    session.close()

    # --- BIET THU (neu co flag --biet-thu) ---
    if args.biet_thu:
        print(f"\n{'=' * 60}")
        print(f"CRAWL BIET THU / LIEN KE — {len(KHU_VUC)} khu vuc")
        print(f"{'=' * 60}\n")

        bt_results = run_crawl_biet_thu(KHU_VUC, session)
        if bt_results:
            bt_timestamp = datetime.now().strftime("%d/%m/%Y %H:%M")
            # Luu rieng voi prefix BT_
            bt_csv = OUTPUT_DIR / "du_lieu_tho_crawl_biet_thu.csv"
            save_csv(bt_results, bt_csv)
            bt_history = OUTPUT_DIR / "lich_su_chi_so_biet_thu.csv"
            save_history(bt_results, bt_history)
            print(f"\n==> Ket qua biet thu: {bt_csv}")
            print(f"==> Lich su biet thu: {bt_history}")


if __name__ == "__main__":
    main()
