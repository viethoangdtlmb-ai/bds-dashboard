# -*- coding: utf-8 -*-
"""
ĐẨY DỮ LIỆU LÊN GOOGLE SHEETS
================================
Đọc dữ liệu từ lich_su_chi_so_phai_sinh.csv → gửi lên Google Sheets
qua webhook Google Apps Script (dùng GET request).

Cách chạy:
  python gui_google_sheets.py
"""

import sys
import io
import csv
import json
import urllib.parse
from pathlib import Path
from datetime import datetime
from time import sleep

# Fix Windows console encoding
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import os
OUTPUT_DIR = Path(os.environ["BDS_DATA_DIR"]) if "BDS_DATA_DIR" in os.environ else Path(__file__).parent


def load_config():
    """Đọc config webhook URL."""
    config_path = OUTPUT_DIR / "google_sheets_config.json"
    if not config_path.exists():
        print("!! Chưa có file google_sheets_config.json")
        print("   Xem hướng dẫn: HUONG_DAN_GOOGLE_SHEETS.md")
        return None
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    if not config.get("enabled"):
        print("!! Google Sheets đang TẮT (enabled: false)")
        print("   Sửa google_sheets_config.json → enabled: true")
        return None
    if not config.get("webhook_url"):
        print("!! Chưa có webhook URL")
        print("   Xem hướng dẫn: HUONG_DAN_GOOGLE_SHEETS.md")
        return None
    return config


def load_today_data():
    """Đọc dữ liệu mới nhất (ngày hôm nay) từ CSV."""
    filepath = OUTPUT_DIR / "lich_su_chi_so_phai_sinh.csv"
    if not filepath.exists():
        print(f"!! Không tìm thấy: {filepath}")
        return [], []

    today = datetime.now().strftime("%Y-%m-%d")
    rows = []

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        headers = next(reader, None)
        if not headers:
            return [], []

        for row in reader:
            if row and row[0] == today:
                rows.append(row)

    return headers, rows


def push_to_sheets(webhook_url, headers, rows):
    """Gửi dữ liệu lên Google Sheets qua GET request (từng dòng)."""
    try:
        from curl_cffi import requests as curl_requests
    except ImportError:
        print("!! Cần cài curl_cffi: pip install curl_cffi")
        return None

    success = 0

    # Bước 1: Gửi header trước
    header_str = "|".join(headers)
    url = f"{webhook_url}?action=init&header={urllib.parse.quote(header_str)}"
    try:
        resp = curl_requests.get(url, impersonate="chrome120", allow_redirects=True, timeout=30)
        if resp.status_code == 200:
            print("  → Đã gửi header")
        else:
            print(f"  !! Gửi header lỗi: HTTP {resp.status_code}")
            return None
    except Exception as e:
        print(f"  !! Gửi header lỗi: {e}")
        return None

    sleep(1)

    # Bước 2: Gửi từng dòng dữ liệu
    for i, row in enumerate(rows):
        row_str = "|".join(str(v) for v in row)
        url = f"{webhook_url}?action=add&data={urllib.parse.quote(row_str)}"

        try:
            resp = curl_requests.get(
                url, impersonate="chrome120",
                allow_redirects=True, timeout=30,
            )
            if resp.status_code == 200:
                success += 1
            else:
                print(f"    !! Dòng {i+1}: HTTP {resp.status_code}")
        except Exception as e:
            print(f"    !! Dòng {i+1}: {e}")

        # Progress
        if (i + 1) % 5 == 0 or i == len(rows) - 1:
            print(f"    → {i+1}/{len(rows)} dòng...", flush=True)

        sleep(0.3)  # Tránh rate limit

    return {"status": "ok", "rows": success} if success > 0 else None


def main():
    print("=" * 60)
    print("ĐẨY DỮ LIỆU LÊN GOOGLE SHEETS")
    print("=" * 60)

    config = load_config()
    if not config:
        return

    headers, rows = load_today_data()
    if not rows:
        today = datetime.now().strftime("%Y-%m-%d")
        print(f"!! Không có dữ liệu ngày {today}")
        print("   Chạy crawl_chi_so_thi_truong.py + tinh_chi_so_phai_sinh.py trước.")
        return

    print(f"  → {len(rows)} dòng ngày {rows[0][0]}")
    print(f"  → Đang gửi lên Google Sheets...")

    result = push_to_sheets(config["webhook_url"], headers, rows)

    if result and result.get("status") == "ok":
        print(f"\n✅ Thành công! Đã gửi {result.get('rows', 0)} dòng lên Google Sheets.")
    else:
        print(f"\n❌ Lỗi: {result}")
        print("   Kiểm tra lại webhook URL và quyền truy cập Apps Script.")


if __name__ == "__main__":
    main()
