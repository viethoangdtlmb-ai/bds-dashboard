"""
Tổng hợp tuần — Tính median từ dữ liệu crawl hàng ngày
Đọc lich_su_chi_so.csv → lọc dòng lỗi (0) → tính median 7 ngày → xuất dashboard_data.json
Tính Δ Quan tâm + Δ Tin rao + Δ Giá tháng + Điểm Tiềm năng
"""
import csv
import json
import os
import sys
import statistics
from collections import defaultdict
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'data')
CSV_FILE = os.path.join(DATA_DIR, 'lich_su_chi_so.csv')
OUTPUT_JSON = os.path.join(SCRIPT_DIR, '..', '..', 'APP - BDS', 'data.json')
OUTPUT_JSON_BACKUP = os.path.join(DATA_DIR, 'dashboard_data.json')

def read_csv():
    rows = []
    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows

def parse_float(val):
    if not val or val.strip() == '':
        return None
    try:
        v = float(val)
        return v if v > 0 else None
    except ValueError:
        return None

def calc_period_stats(rows, target_dates):
    district_data = defaultdict(lambda: {'tin': [], 'views': [], 'gia': []})
    for row in rows:
        if row.get('Ngày') not in target_dates:
            continue
        name = row.get('Khu vực', '').strip()
        if not name:
            continue
        tin = parse_float(row.get('Tổng tin bán'))
        views = parse_float(row.get('Lượt xem khu vực'))
        gia = parse_float(row.get('Giá bán TB (tr/m²)'))
        if tin: district_data[name]['tin'].append(tin)
        if views: district_data[name]['views'].append(views)
        if gia: district_data[name]['gia'].append(gia)

    result = {}
    for name, data in district_data.items():
        tin_med = statistics.median(data['tin']) if data['tin'] else 0
        views_med = statistics.median(data['views']) if data['views'] else 0
        gia_med = statistics.median(data['gia']) if data['gia'] else 0
        vt = round(views_med / tin_med, 1) if tin_med > 0 and views_med > 0 else 0
        result[name] = {'views_tin': vt, 'tin': tin_med, 'gia': gia_med}
    return result

def normalize(values):
    if not values:
        return []
    mn, mx = min(values), max(values)
    if mx == mn:
        return [50] * len(values)
    return [round((v - mn) / (mx - mn) * 100, 1) for v in values]

def add_potential_scores(results):
    if not results:
        return
    vt_vals = [d['views_tin'] for d in results]
    cl_vals = [d['cat_lo'] for d in results]
    gia_vals = [d['gia'] for d in results]
    vt_norm = normalize(vt_vals)
    cl_norm = normalize(cl_vals)
    gia_norm = normalize(gia_vals)
    for i, d in enumerate(results):
        score = round((vt_norm[i] + (100 - cl_norm[i]) + (100 - gia_norm[i])) / 3, 1)
        d['potential'] = score

def calculate_median_data(rows, days=7):
    dates = sorted(set(row['Ngày'] for row in rows if row.get('Ngày')))
    recent_dates = dates[-days:] if len(dates) >= days else dates
    total_days = len(dates)

    # Previous 7 days for Δ weekly
    prev_week = []
    if total_days >= days * 2:
        prev_week = dates[-(days*2):-days]
    elif total_days > days:
        prev_week = dates[:total_days-days]

    # Previous 30 days for Δ monthly (gia tốc giá tháng)
    prev_month = []
    if total_days >= 30:
        month_start = max(0, total_days - 30)
        month_end = max(0, total_days - 23)  # ~7 days, 30 days ago
        prev_month = dates[month_start:month_end]

    prev_week_stats = calc_period_stats(rows, prev_week) if prev_week else {}
    prev_month_stats = calc_period_stats(rows, prev_month) if prev_month else {}

    district_data = defaultdict(lambda: {
        'tin': [], 'views': [], 'gia': [], 'gia_cc': [], 'yoy': [], 'cat_lo': []
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
        gia_cc = parse_float(row.get('Giá chung cư (tr/m²)'))
        yoy = parse_float(row.get('% Tăng giá YoY'))
        cat_lo = parse_float(row.get('% Cắt lỗ'))
        if tin: district_data[name]['tin'].append(tin)
        if views: district_data[name]['views'].append(views)
        if gia: district_data[name]['gia'].append(gia)
        if gia_cc: district_data[name]['gia_cc'].append(gia_cc)
        if yoy: district_data[name]['yoy'].append(yoy)
        if cat_lo: district_data[name]['cat_lo'].append(cat_lo)

    results = []
    for name, data in district_data.items():
        def safe_median(arr):
            return round(statistics.median(arr), 1) if arr else 0

        tin_med = safe_median(data['tin'])
        views_med = safe_median(data['views'])
        gia_med = safe_median(data['gia'])
        gia_cc_med = safe_median(data['gia_cc'])
        yoy_med = safe_median(data['yoy'])
        cat_lo_med = safe_median(data['cat_lo'])

        views_tin = round(views_med / tin_med, 1) if tin_med > 0 and views_med > 0 else 0
        heat = round(views_med * 0.3 + tin_med * 0.2 + views_tin * 20, 0) if views_med > 0 else 0

        cycle = 50
        if yoy_med > 30: cycle = 70 + min(yoy_med - 30, 20)
        elif yoy_med > 15: cycle = 50 + (yoy_med - 15)
        elif yoy_med > 0: cycle = 30 + yoy_med

        # Δ Quan tâm (tuần)
        delta = None
        if name in prev_week_stats and prev_week_stats[name]['views_tin'] > 0 and views_tin > 0:
            delta = round((views_tin - prev_week_stats[name]['views_tin']) / prev_week_stats[name]['views_tin'] * 100, 1)

        # Δ Tin rao (tuần)
        delta_tin = None
        if name in prev_week_stats and prev_week_stats[name]['tin'] > 0 and tin_med > 0:
            delta_tin = round((tin_med - prev_week_stats[name]['tin']) / prev_week_stats[name]['tin'] * 100, 1)

        # Δ Giá tháng (gia tốc giá 30 ngày)
        delta_gia = None
        if name in prev_month_stats and prev_month_stats[name]['gia'] > 0 and gia_med > 0:
            delta_gia = round((gia_med - prev_month_stats[name]['gia']) / prev_month_stats[name]['gia'] * 100, 1)

        if gia_med > 0:
            results.append({
                'name': name,
                'gia': gia_med,
                'gia_cc': gia_cc_med if gia_cc_med > 0 else None,
                'heat': heat,
                'cycle': round(cycle),
                'views_tin': views_tin,
                'delta': delta,
                'delta_tin': delta_tin,
                'delta_gia': delta_gia,
                'yoy': yoy_med,
                'cat_lo': cat_lo_med,
                'tin': tin_med,
                'views': views_med,
            })

    add_potential_scores(results)
    results.sort(key=lambda x: x['views_tin'], reverse=True)

    has_delta = len(prev_week) > 0
    has_delta_gia = len(prev_month) > 0

    return results, recent_dates, has_delta, has_delta_gia

def build_weekly_history(rows, max_weeks=12):
    """Tạo lịch sử tuần cho biểu đồ xu hướng."""
    dates = sorted(set(row['Ngày'] for row in rows if row.get('Ngày')))
    if len(dates) < 2:
        return {}

    # Chia dates thành các tuần (7 ngày/tuần)
    weeks = []
    for i in range(0, len(dates), 7):
        week_dates = dates[i:i+7]
        if week_dates:
            weeks.append(week_dates)

    # Giới hạn max_weeks
    weeks = weeks[-max_weeks:]

    # Mỗi quận: danh sách giá trị theo tuần
    district_history = defaultdict(lambda: {
        'labels': [], 'gia': [], 'gia_cc': [], 'tin': [], 'views_tin': []
    })

    for week_dates in weeks:
        label = week_dates[-1]  # Ngày cuối tuần
        week_stats = calc_period_stats(rows, week_dates)

        # Lấy thêm gia_cc
        district_cc = defaultdict(list)
        for row in rows:
            if row.get('Ngày') not in week_dates:
                continue
            name = row.get('Khu vực', '').strip()
            gia_cc = parse_float(row.get('Giá chung cư (tr/m²)'))
            if name and gia_cc:
                district_cc[name].append(gia_cc)

        for name, stats in week_stats.items():
            h = district_history[name]
            h['labels'].append(label)
            h['gia'].append(round(stats['gia'], 1) if stats['gia'] else None)
            cc_vals = district_cc.get(name, [])
            h['gia_cc'].append(round(statistics.median(cc_vals), 1) if cc_vals else None)
            h['tin'].append(round(stats['tin'], 0) if stats['tin'] else None)
            h['views_tin'].append(stats['views_tin'])

    return dict(district_history)


def main():
    print("📊 Tổng hợp dữ liệu tuần (Median)...")
    if not os.path.exists(CSV_FILE):
        print(f"❌ Không tìm thấy file: {CSV_FILE}")
        return

    rows = read_csv()
    print(f"  Đọc được {len(rows)} dòng dữ liệu")

    results, dates, has_delta, has_delta_gia = calculate_median_data(rows)
    print(f"  Tính median từ {len(dates)} ngày: {dates[0]} → {dates[-1]}")
    print(f"  Kết quả: {len(results)} quận/huyện")
    print(f"  Δ Tuần: {'✅' if has_delta else '⏳ cần 14+ ngày'}")
    print(f"  Δ Giá tháng: {'✅' if has_delta_gia else '⏳ cần 30+ ngày'}")

    # Build weekly history for time-series charts
    history = build_weekly_history(rows)
    print(f"  📈 Lịch sử: {len(history)} quận, {max(len(h['labels']) for h in history.values()) if history else 0} tuần")

    output = {
        'updated': datetime.now().strftime('%d/%m/%Y %H:%M'),
        'date_range': f"{dates[0]} → {dates[-1]}",
        'num_days': len(dates),
        'method': 'median',
        'has_delta': has_delta,
        'has_delta_gia': has_delta_gia,
        'districts': results,
        'history': history
    }

    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  ✅ Xuất: {OUTPUT_JSON}")

    with open(OUTPUT_JSON_BACKUP, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"  ✅ Backup: {OUTPUT_JSON_BACKUP}")

    print("\n  📋 Top 5 Tiềm năng:")
    for d in sorted(results, key=lambda x: x.get('potential', 0), reverse=True)[:5]:
        dg = f", ΔGiá={d['delta_gia']:+.1f}%" if d.get('delta_gia') is not None else ""
        print(f"    {d['name']}: {d.get('potential',0)}/100{dg}")

if __name__ == '__main__':
    main()

