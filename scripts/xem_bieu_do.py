# -*- coding: utf-8 -*-
"""
TẠO BIỂU ĐỒ HTML TƯƠNG TÁC — THỊ TRƯỜNG BĐS HÀ NỘI
=======================================================
Đọc dữ liệu từ lich_su_chi_so_phai_sinh.csv → tạo file HTML
với biểu đồ Chart.js mở bằng trình duyệt.

Cách chạy:
  python xem_bieu_do.py
  → Tự động mở file BIEU_DO_THI_TRUONG.html trong trình duyệt
"""

import sys
import io
import csv
import json
import webbrowser
from pathlib import Path
from datetime import datetime

# Fix Windows console encoding
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

OUTPUT_DIR = Path(__file__).parent


def _float(val):
    if val is None or val == "":
        return None
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return None


def load_history(filename="lich_su_chi_so_phai_sinh.csv"):
    """Đọc toàn bộ dữ liệu lịch sử."""
    filepath = OUTPUT_DIR / filename
    if not filepath.exists():
        print(f"!! Không tìm thấy file: {filepath}")
        return [], [], {}

    rows = []
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    if not rows:
        return [], [], {}

    # Extract unique dates and regions (preserving order)
    dates = []
    regions = []
    seen_dates = set()
    seen_regions = set()

    for row in rows:
        d = row.get("Ngày", "")
        r = row.get("Khu vực", "")
        if d and d not in seen_dates:
            dates.append(d)
            seen_dates.add(d)
        if r and r not in seen_regions:
            regions.append(r)
            seen_regions.add(r)

    # Build lookup: (date, region) -> data
    lookup = {}
    for row in rows:
        key = (row.get("Ngày", ""), row.get("Khu vực", ""))
        lookup[key] = {
            "tin": _float(row.get("Tổng tin")),
            "views": _float(row.get("Tổng views")),
            "views_tin": _float(row.get("Views/Tin")),
            "gia": _float(row.get("Giá (tr/m²)")),
            "cycle": _float(row.get("Cycle")),
            "heat": _float(row.get("Heat Score")),
            "mfv": _float(row.get("MFV")),
            "mfsi": _float(row.get("MFSI")),
            "cung_cau": _float(row.get("Cung-Cầu")),
            "cat_lo": _float(row.get("Cắt lỗ (%)")),
            "yoy": _float(row.get("YoY (%)")),
        }

    return dates, regions, lookup


def generate_html(dates, regions, lookup, bt_dates=None, bt_regions=None, bt_lookup=None):
    """Tạo HTML với biểu đồ Chart.js."""
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M")
    has_bt = bt_regions and len(bt_regions) > 0

    # Prepare data for each region
    region_data = {}
    for region in regions:
        data = {
            "tin": [], "views": [], "views_tin": [], "gia": [],
            "cycle": [], "heat": [], "mfv": [], "mfsi": [],
            "cung_cau": [], "cat_lo": [], "yoy": [],
        }
        for date in dates:
            entry = lookup.get((date, region), {})
            for key in data:
                data[key].append(entry.get(key))
        region_data[region] = data

    # Get latest values for summary cards
    latest_date = dates[-1] if dates else "N/A"
    summary = []
    for region in regions:
        entry = lookup.get((latest_date, region), {})
        summary.append({
            "name": region,
            "tin": entry.get("tin", 0),
            "views": entry.get("views", 0),
            "views_tin": entry.get("views_tin", 0),
            "gia": entry.get("gia", 0),
            "cycle": entry.get("cycle", 0),
            "heat": entry.get("heat", 0),
            "mfsi": entry.get("mfsi", 0),
            "cat_lo": entry.get("cat_lo", 0),
        })

    # Sort summary by heat score descending
    summary.sort(key=lambda x: x.get("heat", 0) or 0, reverse=True)

    # Prepare biệt thự data (if available)
    bt_region_data = {}
    bt_summary = []
    if has_bt:
        for region in bt_regions:
            data = {
                "tin": [], "views": [], "views_tin": [], "gia": [],
                "cycle": [], "heat": [], "mfv": [], "mfsi": [],
                "cung_cau": [], "cat_lo": [], "yoy": [],
            }
            for date in bt_dates:
                entry = bt_lookup.get((date, region), {})
                for key in data:
                    data[key].append(entry.get(key))
            bt_region_data[region] = data

        bt_latest_date = bt_dates[-1] if bt_dates else "N/A"
        for region in bt_regions:
            entry = bt_lookup.get((bt_latest_date, region), {})
            bt_summary.append({
                "name": region,
                "tin": entry.get("tin", 0),
                "views": entry.get("views", 0),
                "views_tin": entry.get("views_tin", 0),
                "gia": entry.get("gia", 0),
                "cycle": entry.get("cycle", 0),
                "heat": entry.get("heat", 0),
                "mfsi": entry.get("mfsi", 0),
                "cat_lo": entry.get("cat_lo", 0),
            })
        bt_summary.sort(key=lambda x: x.get("heat", 0) or 0, reverse=True)

    # Color palette
    colors = [
        "#2563eb", "#dc2626", "#16a34a", "#ea580c", "#9333ea",
        "#0891b2", "#ca8a04", "#e11d48", "#4f46e5", "#059669",
        "#d97706", "#7c3aed", "#0d9488", "#be185d", "#6366f1",
    ]

    html = f"""<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>📊 Biểu đồ Thị trường BĐS Hà Nội</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

        * {{ margin: 0; padding: 0; box-sizing: border-box; }}

        body {{
            font-family: 'Inter', -apple-system, sans-serif;
            background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
            color: #e2e8f0;
            min-height: 100vh;
            padding: 24px;
        }}

        .header {{
            text-align: center;
            margin-bottom: 32px;
            padding: 32px;
            background: linear-gradient(135deg, rgba(37,99,235,0.15), rgba(147,51,234,0.15));
            border-radius: 20px;
            border: 1px solid rgba(99,102,241,0.2);
            backdrop-filter: blur(10px);
        }}

        .header h1 {{
            font-size: 36px;
            font-weight: 800;
            background: linear-gradient(135deg, #60a5fa, #a78bfa, #f472b6);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 8px;
        }}

        .header p {{
            color: #94a3b8;
            font-size: 16px;
        }}

        /* Summary Cards */
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 16px;
            margin-bottom: 32px;
        }}

        .card {{
            background: linear-gradient(135deg, rgba(30,41,59,0.8), rgba(15,23,42,0.9));
            border-radius: 16px;
            padding: 20px;
            border: 1px solid rgba(99,102,241,0.15);
            transition: all 0.3s ease;
            cursor: pointer;
        }}

        .card:hover {{
            border-color: rgba(99,102,241,0.4);
            transform: translateY(-2px);
            box-shadow: 0 8px 32px rgba(99,102,241,0.15);
        }}

        .card-name {{
            font-size: 20px;
            font-weight: 700;
            margin-bottom: 12px;
            color: #f1f5f9;
        }}

        .card-stats {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
        }}

        .stat {{
            display: flex;
            flex-direction: column;
        }}

        .stat-label {{
            font-size: 13px;
            color: #64748b;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        .stat-value {{
            font-size: 24px;
            font-weight: 700;
        }}

        .stat-value.heat {{ color: #f97316; }}
        .stat-value.cycle {{ color: #22d3ee; }}
        .stat-value.views {{ color: #a78bfa; }}
        .stat-value.price {{ color: #4ade80; }}

        /* Tabs */
        .tabs {{
            display: flex;
            gap: 8px;
            margin-bottom: 24px;
            flex-wrap: wrap;
        }}

        .tab-btn {{
            padding: 12px 24px;
            border: 1px solid rgba(99,102,241,0.2);
            background: rgba(30,41,59,0.6);
            color: #94a3b8;
            border-radius: 10px;
            cursor: pointer;
            font-size: 16px;
            font-weight: 500;
            transition: all 0.2s;
        }}

        .tab-btn:hover {{
            border-color: rgba(99,102,241,0.4);
            color: #e2e8f0;
        }}

        .tab-btn.active {{
            background: linear-gradient(135deg, #4f46e5, #7c3aed);
            border-color: transparent;
            color: #fff;
            font-weight: 600;
        }}

        /* Charts */
        .charts-section {{
            margin-bottom: 40px;
        }}

        .section-title {{
            font-size: 24px;
            font-weight: 700;
            margin-bottom: 20px;
            padding-left: 12px;
            border-left: 4px solid #6366f1;
        }}

        .chart-container {{
            background: rgba(30,41,59,0.6);
            border-radius: 16px;
            padding: 24px;
            border: 1px solid rgba(99,102,241,0.1);
            margin-bottom: 20px;
        }}

        .chart-container.hidden {{ display: none; }}

        .chart-title {{
            font-size: 20px;
            font-weight: 600;
            margin-bottom: 16px;
            color: #cbd5e1;
            display: flex;
            align-items: center;
            gap: 12px;
            flex-wrap: wrap;
        }}
        .chart-btns {{ margin-left: auto; display: flex; gap: 6px; }}
        .chart-btns button {{
            padding: 4px 12px; border-radius: 6px; font-size: 12px; cursor: pointer; border: 1px solid rgba(99,102,241,0.3); background: rgba(30,41,59,0.8); color: #94a3b8;
        }}
        .chart-btns button:hover {{ background: rgba(99,102,241,0.2); color: #e2e8f0; }}

        canvas {{ width: 100% !important; }}
        .chart-wrapper {{ position: relative; height: 500px; width: 100%; }}
        @media (max-width: 768px) {{ .chart-wrapper {{ height: 450px; }} }}

        /* Compare section */
        .compare-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }}

        @media (max-width: 768px) {{
            .compare-grid {{ grid-template-columns: 1fr; }}
            .summary-grid {{ grid-template-columns: 1fr; }}
            body {{ padding: 12px; }}
        }}

        .footer {{
            text-align: center;
            padding: 24px;
            color: #475569;
            font-size: 12px;
            margin-top: 40px;
        }}

        /* Property type toggle */
        .prop-toggle {{
            display: flex;
            gap: 8px;
            margin-bottom: 24px;
            justify-content: center;
        }}
        .prop-btn {{
            padding: 14px 32px;
            border: 2px solid rgba(99,102,241,0.3);
            background: rgba(30,41,59,0.6);
            color: #94a3b8;
            border-radius: 12px;
            cursor: pointer;
            font-size: 18px;
            font-weight: 600;
            transition: all 0.3s;
        }}
        .prop-btn:hover {{ border-color: rgba(99,102,241,0.5); color: #e2e8f0; }}
        .prop-btn.active {{
            background: linear-gradient(135deg, #4f46e5, #7c3aed);
            border-color: transparent;
            color: #fff;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 BIỂU ĐỒ THỊ TRƯỜNG BĐS HÀ NỘI</h1>
        <p>Nguồn: batdongsan.com.vn | Cập nhật: {timestamp} | {len(regions)} quận/huyện</p>
    </div>

    <!-- Property type toggle -->
    """ + ("""
    <div class="prop-toggle">
        <button class="prop-btn active" onclick="switchProp('cc')">&#127970; Chung c&#432;</button>
        <button class="prop-btn" onclick="switchProp('bt')">&#127968; Bi&#7879;t th&#7921;</button>
    </div>
    """ if has_bt else "") + f"""

    <!-- Summary Cards -->
    <div class="section-title">🏆 Tổng quan (sắp xếp theo Heat Score)</div>
    <div class="summary-grid" id="summaryGrid"></div>

    <!-- Tab buttons -->
    <div class="section-title">📈 Biểu đồ so sánh</div>
    <div class="tabs">
        <button class="tab-btn active" onclick="showTab('compare')">So sánh tất cả</button>
        <button class="tab-btn" onclick="showTab('detail')">Chi tiết từng khu vực</button>
        <button class="tab-btn" onclick="showTab('compare2')">🔀 Đối chiếu 2 KV</button>
    </div>

    <!-- Compare Charts -->
    <div id="tab-compare" class="charts-section">
        <div class="compare-grid">
            <div class="chart-container">
                <div class="chart-title">🏢 Tổng tin đăng bán <span class="chart-btns"><button onclick="toggleChart(0,false)">Bỏ chọn</button><button onclick="toggleChart(0,true)">Chọn lại</button></span></div>
                <div class="chart-wrapper"><canvas id="chartTin"></canvas></div>
            </div>
            <div class="chart-container">
                <div class="chart-title">👁️ Tổng lượt xem <span class="chart-btns"><button onclick="toggleChart(1,false)">Bỏ chọn</button><button onclick="toggleChart(1,true)">Chọn lại</button></span></div>
                <div class="chart-wrapper"><canvas id="chartViews"></canvas></div>
            </div>
            <div class="chart-container">
                <div class="chart-title">💧 Views/Tin <span class="chart-btns"><button onclick="toggleChart(2,false)">Bỏ chọn</button><button onclick="toggleChart(2,true)">Chọn lại</button></span></div>
                <div class="chart-wrapper"><canvas id="chartViewsTin"></canvas></div>
            </div>
            <div class="chart-container">
                <div class="chart-title">💰 Giá bán TB (tr/m²) <span class="chart-btns"><button onclick="toggleChart(3,false)">Bỏ chọn</button><button onclick="toggleChart(3,true)">Chọn lại</button></span></div>
                <div class="chart-wrapper"><canvas id="chartGia"></canvas></div>
            </div>
            <div class="chart-container">
                <div class="chart-title">📊 Cycle Index <span class="chart-btns"><button onclick="toggleChart(4,false)">Bỏ chọn</button><button onclick="toggleChart(4,true)">Chọn lại</button></span></div>
                <div class="chart-wrapper"><canvas id="chartCycle"></canvas></div>
            </div>
            <div class="chart-container">
                <div class="chart-title">🔥 Heat Score <span class="chart-btns"><button onclick="toggleChart(5,false)">Bỏ chọn</button><button onclick="toggleChart(5,true)">Chọn lại</button></span></div>
                <div class="chart-wrapper"><canvas id="chartHeat"></canvas></div>
            </div>
        </div>
    </div>

    <!-- Detail Charts -->
    <div id="tab-detail" class="charts-section" style="display:none">
        <div class="tabs" id="regionTabs"></div>
        <div id="detailCharts"></div>
    </div>

    <!-- Compare 2 KV -->
    <div id="tab-compare2" class="charts-section" style="display:none">
        <div style="display:flex; gap:16px; margin-bottom:24px; flex-wrap:wrap; align-items:center;">
            <select id="cmp1" style="padding:12px 20px; border-radius:10px; background:rgba(30,41,59,0.8); color:#e2e8f0; border:1px solid rgba(99,102,241,0.3); font-size:16px; cursor:pointer;">
            </select>
            <span style="font-size:20px; color:#6366f1; font-weight:700;">VS</span>
            <select id="cmp2" style="padding:12px 20px; border-radius:10px; background:rgba(30,41,59,0.8); color:#e2e8f0; border:1px solid rgba(99,102,241,0.3); font-size:16px; cursor:pointer;">
            </select>
            <button onclick="renderCompare2()" style="padding:12px 24px; border-radius:10px; background:linear-gradient(135deg,#4f46e5,#7c3aed); color:#fff; border:none; font-size:16px; font-weight:600; cursor:pointer;">So sánh</button>
        </div>
        <div id="compare2Results"></div>
    </div>

    <div class="footer">
        🤖 Tự động tạo bởi xem_bieu_do.py | Dữ liệu: batdongsan.com.vn | {timestamp}
    </div>

    <script>
    const DATES = {json.dumps(dates)};
    const REGIONS = {json.dumps(regions, ensure_ascii=False)};
    const DATA = {json.dumps(region_data, ensure_ascii=False)};
    const SUMMARY = {json.dumps(summary, ensure_ascii=False)};
    const COLORS = {json.dumps(colors)};
    const HAS_BT = {'true' if has_bt else 'false'};
    {f'''
    const BT_DATES = {json.dumps(bt_dates, ensure_ascii=False) if bt_dates else '[]'};
    const BT_REGIONS = {json.dumps(list(bt_region_data.keys()), ensure_ascii=False) if has_bt else '[]'};
    const BT_DATA = {json.dumps(bt_region_data, ensure_ascii=False) if has_bt else '{{}}'};
    const BT_SUMMARY = {json.dumps(bt_summary, ensure_ascii=False) if has_bt else '[]'};
    ''' if has_bt else ''}

    // Chart.js defaults
    Chart.defaults.color = '#94a3b8';
    Chart.defaults.borderColor = 'rgba(99,102,241,0.1)';
    Chart.defaults.font.family = 'Inter';
    Chart.defaults.font.size = 14;

    // --- Summary Cards ---
    const grid = document.getElementById('summaryGrid');
    SUMMARY.forEach((s, i) => {{
        const cycleLabel = s.cycle >= 85 ? '🍂 Quá nóng' :
                          s.cycle >= 60 ? '☀️ Đang tăng' :
                          s.cycle >= 30 ? '🌱 Bắt đầu' : '🥶 Đáy';
        grid.innerHTML += `
            <div class="card" onclick="showRegionDetail('${{s.name}}')">
                <div class="card-name">#${{i+1}} ${{s.name}}</div>
                <div class="card-stats">
                    <div class="stat">
                        <span class="stat-label">Heat Score</span>
                        <span class="stat-value heat">${{s.heat?.toFixed(0) || 'N/A'}}</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Cycle</span>
                        <span class="stat-value cycle">${{s.cycle?.toFixed(0) || 'N/A'}} <span style="font-size:13px;font-weight:500;opacity:0.8">${{cycleLabel}}</span></span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Giá (tr/m²)</span>
                        <span class="stat-value price">${{s.gia?.toFixed(1) || 'N/A'}}</span>
                    </div>
                    <div class="stat">
                        <span class="stat-label">Views/Tin</span>
                        <span class="stat-value views">${{s.views_tin?.toFixed(1) || 'N/A'}}</span>
                    </div>
                </div>
            </div>`;
    }});

    const allCharts = [];
    // --- Bar Charts (compare all regions) ---
    function createBarChart(canvasId, dataKey, label, color) {{
        const ctx = document.getElementById(canvasId).getContext('2d');
        const sortedRegions = [...SUMMARY];
        const labels = sortedRegions.map(s => s.name);
        const values = sortedRegions.map(s => {{
            const d = DATA[s.name];
            const arr = d[dataKey];
            return arr[arr.length - 1];
        }});

        const chart = new Chart(ctx, {{
            type: DATES.length > 1 ? 'line' : 'bar',
            data: DATES.length > 1 ? {{
                labels: DATES,
                datasets: REGIONS.map((r, i) => ({{
                    label: r,
                    data: DATA[r][dataKey],
                    borderColor: COLORS[i % COLORS.length],
                    backgroundColor: COLORS[i % COLORS.length] + '20',
                    borderWidth: 2,
                    tension: 0.3,
                    pointRadius: 3,
                }}))
            }} : {{
                labels: labels,
                datasets: [{{
                    label: label,
                    data: values,
                    backgroundColor: labels.map((_, i) => COLORS[i % COLORS.length] + '90'),
                    borderColor: labels.map((_, i) => COLORS[i % COLORS.length]),
                    borderWidth: 1.5,
                    borderRadius: 6,
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        display: DATES.length > 1,
                        position: 'bottom',
                        labels: {{ boxWidth: 12, padding: 8, font: {{ size: 14 }}, usePointStyle: true }}
                    }},
                }},
                scales: {{
                    y: {{ beginAtZero: dataKey !== 'gia', grid: {{ color: 'rgba(99,102,241,0.05)' }}, ticks: {{ font: {{ size: 12 }} }} }},
                    x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 14 }}, maxRotation: 90, minRotation: 45, autoSkip: false }} }}
                }}
            }}
        }});
        allCharts.push(chart);
    }}

    function toggleAllDatasets(show) {{
        allCharts.forEach(chart => {{
            chart.data.datasets.forEach((ds, i) => {{
                chart.setDatasetVisibility(i, show);
            }});
            chart.update();
        }});
    }}

    function toggleChart(idx, show) {{
        const chart = allCharts[idx];
        if (!chart) return;
        chart.data.datasets.forEach((ds, i) => {{
            chart.setDatasetVisibility(i, show);
        }});
        chart.update();
    }}

    createBarChart('chartTin', 'tin', 'Tổng tin', '#2563eb');
    createBarChart('chartViews', 'views', 'Tổng views', '#9333ea');
    createBarChart('chartViewsTin', 'views_tin', 'Views/Tin', '#ca8a04');
    createBarChart('chartGia', 'gia', 'Giá (tr/m²)', '#16a34a');
    createBarChart('chartCycle', 'cycle', 'Cycle Index', '#0891b2');
    createBarChart('chartHeat', 'heat', 'Heat Score', '#ea580c');

    // --- Tabs ---
    function showTab(tab) {{
        document.querySelectorAll('.tabs')[0].querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        event.target.classList.add('active');
        document.getElementById('tab-compare').style.display = tab === 'compare' ? 'block' : 'none';
        document.getElementById('tab-detail').style.display = tab === 'detail' ? 'block' : 'none';
        document.getElementById('tab-compare2').style.display = tab === 'compare2' ? 'block' : 'none';
        if (tab === 'detail' && !document.getElementById('regionTabs').innerHTML) {{
            buildRegionTabs();
            showRegionDetail(REGIONS[0]);
        }}
        if (tab === 'compare2' && !document.getElementById('cmp1').options.length) {{
            const s1 = document.getElementById('cmp1');
            const s2 = document.getElementById('cmp2');
            const sorted = [...SUMMARY].sort((a,b) => a.name.localeCompare(b.name, 'vi'));
            sorted.forEach((r, i) => {{
                s1.innerHTML += '<option value="' + r.name + '" ' + (i===0?'selected':'') + '>#' + (i+1) + ' ' + r.name + '</option>';
                s2.innerHTML += '<option value="' + r.name + '" ' + (i===1?'selected':'') + '>#' + (i+1) + ' ' + r.name + '</option>';
            }});
            renderCompare2();
        }}
    }}

    // --- Compare 2 KV ---
    function renderCompare2() {{
        const r1 = document.getElementById('cmp1').value;
        const r2 = document.getElementById('cmp2').value;
        const d1 = DATA[r1], d2 = DATA[r2];
        const s1 = SUMMARY.find(s => s.name === r1) || {{}};
        const s2 = SUMMARY.find(s => s.name === r2) || {{}};

        const container = document.getElementById('compare2Results');
        
        function better(a, b, higher) {{ if (a == null || b == null) return ''; return higher ? (a > b ? '✅' : a < b ? '' : '🟰') : (a < b ? '✅' : a > b ? '' : '🟰'); }}
        function fv(v, d) {{ return v != null ? v.toFixed(d||0) : 'N/A'; }}

        container.innerHTML = `
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:16px; margin-bottom:20px;">
                <div class="card" style="border-color:rgba(37,99,235,0.4);">
                    <div class="card-name" style="color:#60a5fa;">${{r1}}</div>
                    <div class="card-stats">
                        <div class="stat"><span class="stat-label">Heat</span><span class="stat-value heat">${{fv(s1.heat)}}</span></div>
                        <div class="stat"><span class="stat-label">Cycle</span><span class="stat-value cycle">${{fv(s1.cycle)}}</span></div>
                        <div class="stat"><span class="stat-label">MFSI</span><span class="stat-value" style="color:#a78bfa;">${{fv(s1.mfsi,1)}}</span></div>
                        <div class="stat"><span class="stat-label">Giá</span><span class="stat-value price">${{fv(s1.gia,1)}}</span></div>
                        <div class="stat"><span class="stat-label">Views/Tin</span><span class="stat-value views">${{fv(s1.views_tin,1)}}</span></div>
                    </div>
                </div>
                <div class="card" style="border-color:rgba(220,38,38,0.4);">
                    <div class="card-name" style="color:#f87171;">${{r2}}</div>
                    <div class="card-stats">
                        <div class="stat"><span class="stat-label">Heat</span><span class="stat-value heat">${{fv(s2.heat)}}</span></div>
                        <div class="stat"><span class="stat-label">Cycle</span><span class="stat-value cycle">${{fv(s2.cycle)}}</span></div>
                        <div class="stat"><span class="stat-label">MFSI</span><span class="stat-value" style="color:#a78bfa;">${{fv(s2.mfsi,1)}}</span></div>
                        <div class="stat"><span class="stat-label">Giá</span><span class="stat-value price">${{fv(s2.gia,1)}}</span></div>
                        <div class="stat"><span class="stat-label">Views/Tin</span><span class="stat-value views">${{fv(s2.views_tin,1)}}</span></div>
                    </div>
                </div>
            </div>

            <div class="compare-grid">
                <div class="chart-container">
                    <div class="chart-title">🔥 So sánh chỉ số tổng hợp</div>
                    <canvas id="cmp2Radar"></canvas>
                </div>
                <div class="chart-container">
                    <div class="chart-title">💰 Giá & Views theo thời gian</div>
                    <div class="chart-wrapper"><canvas id="cmp2Lines"></canvas></div>
                </div>
            </div>
            <div class="compare-grid">
                <div class="chart-container">
                    <div class="chart-title">📊 Cycle Index theo thời gian</div>
                    <div class="chart-wrapper"><canvas id="cmp2Cycle"></canvas></div>
                </div>
                <div class="chart-container">
                    <div class="chart-title">💰 MFSI theo thời gian</div>
                    <div class="chart-wrapper"><canvas id="cmp2Mfsi"></canvas></div>
                </div>
            </div>
            <div class="compare-grid">
                <div class="chart-container">
                    <div class="chart-title">🔥 Heat Score theo thời gian</div>
                    <div class="chart-wrapper"><canvas id="cmp2Heat"></canvas></div>
                </div>
                <div class="chart-container">
                    <div class="chart-title">💸 MFV theo thời gian</div>
                    <div class="chart-wrapper"><canvas id="cmp2Mfv"></canvas></div>
                </div>
            </div>
            <div class="compare-grid">
                <div class="chart-container">
                    <div class="chart-title">⚖️ Cung-Cầu theo thời gian</div>
                    <div class="chart-wrapper"><canvas id="cmp2Cc"></canvas></div>
                </div>
                <div class="chart-container">
                    <div class="chart-title">📉 % Cắt lỗ theo thời gian</div>
                    <div class="chart-wrapper"><canvas id="cmp2CatLo"></canvas></div>
                </div>
            </div>`;

        // Radar chart
        const metrics = ['heat','cycle','views_tin','gia','mfsi','cat_lo'];
        const labels = ['Heat Score','Cycle','Views/Tin','Giá (tr/m²)','MFSI','Cắt lỗ (%)'];
        const maxVals = [800, 100, 20, 200, 100, 20];
        const v1 = metrics.map((m,i) => ((s1[m]||0)/maxVals[i]*100));
        const v2 = metrics.map((m,i) => ((s2[m]||0)/maxVals[i]*100));

        new Chart(document.getElementById('cmp2Radar'), {{
            type: 'radar',
            data: {{
                labels: labels,
                datasets: [
                    {{ label: r1, data: v1, borderColor: '#2563eb', backgroundColor: '#2563eb30', borderWidth: 2, pointRadius: 4 }},
                    {{ label: r2, data: v2, borderColor: '#dc2626', backgroundColor: '#dc262630', borderWidth: 2, pointRadius: 4 }},
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{ legend: {{ position: 'bottom' }} }},
                scales: {{ r: {{ beginAtZero: true, max: 100, ticks: {{ display: false }}, grid: {{ color: 'rgba(99,102,241,0.1)' }}, pointLabels: {{ font: {{ size: 13 }}, color: '#94a3b8' }} }} }}
            }}
        }});


        // Line chart
        new Chart(document.getElementById('cmp2Lines'), {{
            type: DATES.length > 1 ? 'line' : 'bar',
            data: {{
                labels: DATES,
                datasets: [
                    {{ label: r1+' Giá', data: d1.gia, borderColor: '#2563eb', borderWidth: 2.5, tension: 0.3, yAxisID: 'y' }},
                    {{ label: r2+' Giá', data: d2.gia, borderColor: '#dc2626', borderWidth: 2.5, tension: 0.3, yAxisID: 'y' }},
                    {{ label: r1+' V/Tin', data: d1.views_tin, borderColor: '#60a5fa', borderWidth: 1.5, tension: 0.3, borderDash: [5,5], yAxisID: 'y1' }},
                    {{ label: r2+' V/Tin', data: d2.views_tin, borderColor: '#f87171', borderWidth: 1.5, tension: 0.3, borderDash: [5,5], yAxisID: 'y1' }},
                ]
            }},
            options: {{
                responsive: true,
                interaction: {{ mode: 'index', intersect: false }},
                plugins: {{ legend: {{ position: 'bottom', labels: {{ boxWidth: 14 }} }} }},
                scales: {{
                    y: {{ position: 'left', title: {{ display: true, text: 'Giá (tr/m²)' }}, grid: {{ color: 'rgba(99,102,241,0.05)' }} }},
                    y1: {{ position: 'right', title: {{ display: true, text: 'Views/Tin' }}, grid: {{ display: false }} }},
                    x: {{ grid: {{ display: false }} }}
                }}
            }}
        }});

        // Cycle & MFSI line charts
        function cmpLineChart(canvasId, dataKey, label, color1, color2, maxY) {{
            const yScale = {{ beginAtZero: true, grid: {{ color: 'rgba(99,102,241,0.05)' }}, ticks: {{ font: {{ size: 12 }} }}, title: {{ display: true, text: label, font: {{ size: 13 }}, color: '#94a3b8' }} }};
            if (maxY) yScale.max = maxY;
            new Chart(document.getElementById(canvasId), {{
                type: DATES.length > 1 ? 'line' : 'bar',
                data: {{
                    labels: DATES,
                    datasets: [
                        {{ label: r1, data: d1[dataKey], borderColor: color1, backgroundColor: color1+'20', borderWidth: 2.5, tension: 0.3, pointRadius: 4, fill: true }},
                        {{ label: r2, data: d2[dataKey], borderColor: color2, backgroundColor: color2+'20', borderWidth: 2.5, tension: 0.3, pointRadius: 4, fill: true }},
                    ]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{ legend: {{ position: 'bottom', labels: {{ font: {{ size: 14 }}, padding: 12 }} }} }},
                    scales: {{
                        y: yScale,
                        x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 12 }} }} }}
                    }}
                }}
            }});
        }}
        cmpLineChart('cmp2Cycle', 'cycle', 'Cycle (0-100)', '#0891b2', '#ea580c', 100);
        cmpLineChart('cmp2Mfsi', 'mfsi', 'MFSI (0-100)', '#7c3aed', '#e11d48', 100);
        cmpLineChart('cmp2Heat', 'heat', 'Heat Score', '#f59e0b', '#dc2626');
        cmpLineChart('cmp2Mfv', 'mfv', 'MFV', '#10b981', '#8b5cf6');
        cmpLineChart('cmp2Cc', 'cung_cau', 'Cung-Cầu', '#0ea5e9', '#f43f5e');
        cmpLineChart('cmp2CatLo', 'cat_lo', 'Cắt lỗ (%)', '#f97316', '#6366f1');
    }}

    // --- Detail Charts ---
    let detailChart = null;

    function buildRegionTabs() {{
        const container = document.getElementById('regionTabs');
        REGIONS.forEach((r, i) => {{
            container.innerHTML += `<button class="tab-btn ${{i===0?'active':''}}" onclick="showRegionDetail('${{r}}'); this.parentElement.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active')); this.classList.add('active');">${{r}}</button>`;
        }});
    }}

    function showRegionDetail(region) {{
        const container = document.getElementById('detailCharts');
        container.innerHTML = `
            <div class="chart-container">
                <div class="chart-title">📊 ${{region}} — Xu hướng theo thời gian</div>
                <canvas id="detailCanvas" height="120"></canvas>
            </div>
            <div class="compare-grid">
                <div class="chart-container">
                    <div class="chart-title">🏢 Tổng tin & 👁️ Views</div>
                    <canvas id="detailTinViews" height="100"></canvas>
                </div>
                <div class="chart-container">
                    <div class="chart-title">🔥 Heat & Cycle & MFSI</div>
                    <canvas id="detailIndicators" height="100"></canvas>
                </div>
            </div>`;

        const d = DATA[region];

        // Main chart: Giá + Views/Tin (dual axis)
        new Chart(document.getElementById('detailCanvas'), {{
            type: DATES.length > 1 ? 'line' : 'bar',
            data: {{
                labels: DATES,
                datasets: [
                    {{ label: 'Giá (tr/m²)', data: d.gia, borderColor: '#4ade80', backgroundColor: '#4ade8030', borderWidth: 2.5, tension: 0.3, yAxisID: 'y' }},
                    {{ label: 'Views/Tin', data: d.views_tin, borderColor: '#a78bfa', backgroundColor: '#a78bfa30', borderWidth: 2.5, tension: 0.3, yAxisID: 'y1' }},
                    {{ label: 'Cắt lỗ (%)', data: d.cat_lo, borderColor: '#f87171', backgroundColor: '#f8717130', borderWidth: 2, tension: 0.3, borderDash: [5,5], yAxisID: 'y1' }},
                ]
            }},
            options: {{
                responsive: true,
                interaction: {{ mode: 'index', intersect: false }},
                plugins: {{ legend: {{ position: 'bottom', labels: {{ boxWidth: 14, font: {{ size: 14 }} }} }} }},
                scales: {{
                    y: {{ position: 'left', title: {{ display: true, text: 'Giá (tr/m²)' }}, grid: {{ color: 'rgba(99,102,241,0.05)' }} }},
                    y1: {{ position: 'right', title: {{ display: true, text: 'Views/Tin — Cắt lỗ (%)' }}, grid: {{ display: false }} }},
                    x: {{ grid: {{ display: false }} }}
                }}
            }}
        }});

        // Tin & Views
        new Chart(document.getElementById('detailTinViews'), {{
            type: DATES.length > 1 ? 'line' : 'bar',
            data: {{
                labels: DATES,
                datasets: [
                    {{ label: 'Tổng tin', data: d.tin, borderColor: '#2563eb', backgroundColor: '#2563eb30', borderWidth: 2, tension: 0.3 }},
                    {{ label: 'Tổng views', data: d.views, borderColor: '#f97316', backgroundColor: '#f9731630', borderWidth: 2, tension: 0.3 }},
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{ legend: {{ position: 'bottom' }} }},
                scales: {{ y: {{ beginAtZero: true, grid: {{ color: 'rgba(99,102,241,0.05)' }}, ticks: {{ font: {{ size: 13 }} }} }}, x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 13 }} }} }} }}
            }}
        }});

        // Indicators
        new Chart(document.getElementById('detailIndicators'), {{
            type: DATES.length > 1 ? 'line' : 'bar',
            data: {{
                labels: DATES,
                datasets: [
                    {{ label: 'Heat Score', data: d.heat, borderColor: '#f97316', backgroundColor: '#f9731630', borderWidth: 2, tension: 0.3 }},
                    {{ label: 'Cycle', data: d.cycle, borderColor: '#22d3ee', backgroundColor: '#22d3ee30', borderWidth: 2, tension: 0.3 }},
                    {{ label: 'MFSI', data: d.mfsi, borderColor: '#a78bfa', backgroundColor: '#a78bfa30', borderWidth: 2, tension: 0.3 }},
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{ legend: {{ position: 'bottom' }} }},
                scales: {{ y: {{ beginAtZero: true, grid: {{ color: 'rgba(99,102,241,0.05)' }}, ticks: {{ font: {{ size: 13 }} }} }}, x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 13 }} }} }} }}
            }}
        }});

        // Switch to detail tab
        document.getElementById('tab-compare').style.display = 'none';
        document.getElementById('tab-detail').style.display = 'block';
        document.querySelectorAll('.tabs')[0].querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tabs')[0].querySelectorAll('.tab-btn')[1].classList.add('active');
        if (!document.getElementById('regionTabs').innerHTML) buildRegionTabs();
        document.getElementById('regionTabs').querySelectorAll('.tab-btn').forEach(b => {{
            b.classList.remove('active');
            if (b.textContent === region) b.classList.add('active');
        }});
    }}
    </script>
</body>
</html>"""
    return html


def main():
    print("=" * 60)
    print("TẠO BIỂU ĐỒ HTML TƯƠNG TÁC")
    print("=" * 60)

    dates, regions, lookup = load_history()
    if not regions:
        print("!! Không có dữ liệu. Chạy crawl + tính chỉ số trước.")
        return

    print(f"  → Chung cư: {len(regions)} khu vực, {len(dates)} ngày dữ liệu")

    # Load biệt thự data (nếu có)
    bt_dates, bt_regions, bt_lookup = load_history("lich_su_chi_so_biet_thu.csv")
    if bt_regions:
        print(f"  → Biệt thự: {len(bt_regions)} khu vực, {len(bt_dates)} ngày dữ liệu")
    else:
        bt_dates, bt_regions, bt_lookup = None, None, None
        print(f"  → Biệt thự: chưa có dữ liệu")

    html = generate_html(dates, regions, lookup, bt_dates, bt_regions, bt_lookup)
    output_path = OUTPUT_DIR / "BIEU_DO_THI_TRUONG.html"
    output_path.write_text(html, encoding="utf-8")
    print(f"  → File: {output_path}")

    # Tự động mở trong trình duyệt
    webbrowser.open(str(output_path))
    print(f"\n✅ Đã mở biểu đồ trong trình duyệt!")
    print(f"   Nếu không tự mở, hãy double-click file: BIEU_DO_THI_TRUONG.html")


if __name__ == "__main__":
    main()
