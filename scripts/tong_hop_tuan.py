"""
Tổng hợp tuần — Tính median từ dữ liệu crawl hàng ngày
Đọc lich_su_chi_so.csv → lọc dòng lỗi (0) → tính median 7 ngày → xuất dashboard_data.json
"""
import csv
import json
import os
import sys
import statistics
from collections import defaultdict
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8')

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'data')
CSV_FILE = os.path.join(DATA_DIR, 'lich_su_chi_so.csv')
OUTPUT_JSON = os.path.join(SCRIPT_DIR, '..', '..', 'APP - BDS', 'data.json')

# Also output to bds-dashboard for backup
OUTPUT_JSON_BACKUP = os.path.join(DATA_DIR, 'dashboard_data.json')

def read_csv():
    """Read crawl CSV and return list of rows"""
    rows = []
    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows

def parse_float(val):
    """Safely parse float, return None if empty or invalid"""
    if not val or val.strip() == '':
        return None
    try:
        v = float(val)
        return v if v > 0 else None  # Treat 0 as missing data
    except ValueError:
        return None

def calculate_median_data(rows, days=7):
    """Calculate median for each district over last N days"""
    # Get unique dates, sorted
    dates = sorted(set(row['Ngày'] for row in rows if row.get('Ngày')))
    
    # Use last N days
    recent_dates = dates[-days:] if len(dates) >= days else dates
    
    # Group data by district
    district_data = defaultdict(lambda: {
        'tin': [], 'views': [], 'gia': [], 'yoy': [], 'cat_lo': []
    })
    
    for row in rows:
        if row.get('Ngày') not in recent_dates:
            continue
        
        name = row.get('Khu vực', '').strip()
        if not name:
            continue
        
        tin = parse_float(row.get('Tổng tin bán'))
        views = parse_float(row.get('Lượt xem khu vực'))
        gia = parse_float(row.get('Giá bán TB (tr/m²)'))
        yoy = parse_float(row.get('% Tăng giá YoY'))
        cat_lo = parse_float(row.get('% Cắt lỗ'))
        
        if tin is not None:
            district_data[name]['tin'].append(tin)
        if views is not None:
            district_data[name]['views'].append(views)
        if gia is not None:
            district_data[name]['gia'].append(gia)
        if yoy is not None:
            district_data[name]['yoy'].append(yoy)
        if cat_lo is not None:
            district_data[name]['cat_lo'].append(cat_lo)
    
    # Calculate median for each district
    results = []
    for name, data in district_data.items():
        def safe_median(arr):
            return round(statistics.median(arr), 1) if arr else 0
        
        tin_med = safe_median(data['tin'])
        views_med = safe_median(data['views'])
        gia_med = safe_median(data['gia'])
        yoy_med = safe_median(data['yoy'])
        cat_lo_med = safe_median(data['cat_lo'])
        
        # Calculate views/tin (interest level)
        views_tin = round(views_med / tin_med, 1) if tin_med > 0 and views_med > 0 else 0
        
        # Calculate heat score (simplified)
        heat = round(views_med * 0.3 + tin_med * 0.2 + views_tin * 20, 0) if views_med > 0 else 0
        
        # Cycle index (use existing logic from crawl)
        cycle = 50  # Default
        if yoy_med > 30:
            cycle = 70 + min(yoy_med - 30, 20)
        elif yoy_med > 15:
            cycle = 50 + (yoy_med - 15)
        elif yoy_med > 0:
            cycle = 30 + yoy_med
        
        if gia_med > 0:
            results.append({
                'name': name,
                'gia': gia_med,
                'heat': heat,
                'cycle': round(cycle),
                'views_tin': views_tin,
                'yoy': yoy_med,
                'cat_lo': cat_lo_med,
                'tin': tin_med,
                'views': views_med,
            })
    
    # Sort by views_tin (interest level) descending
    results.sort(key=lambda x: x['views_tin'], reverse=True)
    
    return results, recent_dates

def main():
    print("📊 Tổng hợp dữ liệu tuần (Median)...")
    
    if not os.path.exists(CSV_FILE):
        print(f"❌ Không tìm thấy file: {CSV_FILE}")
        return
    
    rows = read_csv()
    print(f"  Đọc được {len(rows)} dòng dữ liệu")
    
    results, dates = calculate_median_data(rows)
    print(f"  Tính median từ {len(dates)} ngày: {dates[0]} → {dates[-1]}")
    print(f"  Kết quả: {len(results)} quận/huyện")
    
    # Output JSON
    output = {
        'updated': datetime.now().strftime('%d/%m/%Y %H:%M'),
        'date_range': f"{dates[0]} → {dates[-1]}",
        'num_days': len(dates),
        'method': 'median',
        'districts': results
    }
    
    # Save to APP - BDS
    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  ✅ Xuất: {OUTPUT_JSON}")
    
    # Save backup
    with open(OUTPUT_JSON_BACKUP, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  ✅ Backup: {OUTPUT_JSON_BACKUP}")
    
    # Print summary
    print("\n  📋 Top 5 Độ quan tâm:")
    for i, d in enumerate(results[:5]):
        print(f"    #{i+1} {d['name']}: Views/Tin={d['views_tin']}, Giá={d['gia']} tr/m²")

if __name__ == '__main__':
    main()
