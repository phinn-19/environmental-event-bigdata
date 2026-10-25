"""Environmental Event Monitoring — Streamlit event-centric dashboard.

Place this file at src/dashboard/app.py in environmental-event-bigdata.
Reads data/events/events.json; LLM is called ONLY when clicked.
"""

from __future__ import annotations

import html
import os
import sys
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st

# Allow `streamlit run src/dashboard/app.py` from the project directory.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.validation import read_json, validate_events
from src.dashboard.services.llm_service import (
    DEFAULT_GEMINI_MODEL,
    DEFAULT_GROQ_MODEL,
    LLMServiceError,
    build_prompt,
    event_fingerprint,
    explain_event,
)


EVENTS_FILE = ROOT / "data" / "events" / "events.json"
AQ = "AIR_QUALITY_ANOMALY"
FIRE = "FIRE_HOTSPOT_CLUSTER"
HOME = "Tổng quan & Bản đồ"
DETAIL = "Chi tiết sự kiện"
LABELS = {AQ: "Bất thường CLKK · OpenAQ", FIRE: "Cụm điểm nóng · FIRMS"}
SHORT_LABELS = {AQ: "OPENAQ / AQ", FIRE: "NASA FIRMS"}
TYPE_COLORS = {AQ: "#20B7C6", FIRE: "#FF9E57"}

st.set_page_config(
    page_title="Environmental Events | Monitoring",
    page_icon="🌏",
    layout="wide",
    initial_sidebar_state="expanded",
)


# -------------------------- DATA & FORMATTERS --------------------------- #

def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def text(value) -> str:
    """Escape dynamic content before inserting it into any HTML snippet."""
    return html.escape(str(value), quote=True)


def fmt(value, unit="", digits=1) -> str:
    if value is None:
        return "Chưa có"
    return f"{value:,.{digits}f}{(' ' + unit) if unit else ''}"


def date_label(iso_utc: str) -> str:
    return parse_utc(iso_utc).strftime("%d/%m/%Y · %H:%M UTC")


def brief_metric(event: dict) -> str:
    metrics = event["metrics"]
    if event["event_type"] == AQ:
        return f"Peak PM2.5 · {metrics['peak_ugm3']:.1f} µg/m³"
    return f"{metrics['hotspot_count']} hotspot · FRP max {fmt(metrics['max_frp_mw'], 'MW')}"


def load_events() -> list[dict]:
    if not EVENTS_FILE.is_file():
        raise FileNotFoundError(
            f"Chưa tìm thấy {EVENTS_FILE}. Hãy chạy bước ghép AQ + FIRMS trước."
        )
    events = read_json(EVENTS_FILE)
    validate_events(events)
    return events


def filter_events(events, selected_types, selected_regions, selected_severities,
                  start_date, end_date):
    if start_date is not None and start_date > end_date:
        return []
    start = (
        datetime.combine(start_date, time.min, tzinfo=timezone.utc)
        if start_date is not None else None
    )
    end = (
        datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=timezone.utc)
        if end_date is not None else None
    )
    result = []
    for event in events:
        if event["event_type"] not in selected_types:
            continue
        if event["region_id"] not in selected_regions:
            continue
        if event["severity"] not in selected_severities:
            continue
        # Overlapping the selected UTC date window (rather than start date only).
        if start is not None and not (
            parse_utc(event["start_time_utc"]) < end
            and parse_utc(event["end_time_utc"]) > start
        ):
            continue
        result.append(event)
    return sorted(result, key=lambda e: (e["start_time_utc"], e["event_id"]), reverse=True)


def open_event(event_id: str):
    st.session_state["event_id"] = event_id
    st.session_state["page"] = DETAIL
    st.session_state["event_picker"] = event_id


def back_to_map():
    st.session_state["page"] = HOME


# ----------------------------- VISUAL STYLE ----------------------------- #

def apply_theme(dark: bool):
    palette = (
        dict(bg="#0B1420", sidebar="#101E2C", card="#162534", text="#ECF5FB",
             muted="#A8BBCA", border="#294054", accent="#4AD5D3", elevated="#1D3244",
             hero_a="#123C4C", hero_b="#12283C", shadow="0 12px 34px rgba(0,0,0,.17)",
             badge="#203B48", hr="#294054")
        if dark else
        dict(bg="#F4F8FB", sidebar="#EAF2F6", card="#FFFFFF", text="#193248",
             muted="#667D8D", border="#DFE8EE", accent="#087C89", elevated="#F0F6F8",
             hero_a="#124D60", hero_b="#123248", shadow="0 10px 30px rgba(17,55,77,.06)",
             badge="#EAF7F8", hr="#DFE8EE")
    )
    st.markdown(f"""
    <style>
      html, body, .stApp {{font-family: 'Segoe UI', Inter, sans-serif;}}
      .stApp {{background: {palette['bg']} !important; color: {palette['text']};}}
      .block-container {{padding-top: 2.1rem; padding-bottom: 2.2rem; max-width: 1500px;}}
      section[data-testid="stSidebar"] {{background: {palette['sidebar']} !important; border-right: 1px solid {palette['border']};}}
      section[data-testid="stSidebar"] .stMarkdown, section[data-testid="stSidebar"] label,
      section[data-testid="stSidebar"] p {{color: {palette['text']};}}
      .stApp h1, .stApp h2, .stApp h3, .stApp h4,
      .stApp p, .stApp label {{color: {palette['text']};}}
      div[data-testid="stMetric"] {{
        background: {palette['card']}; border: 1px solid {palette['border']};
        border-radius: 17px; padding: 18px 19px; box-shadow: {palette['shadow']};
      }}
      div[data-testid="stMetricLabel"] {{color: {palette['muted']} !important;}}
      div[data-testid="stMetricValue"] {{color: {palette['text']} !important;}}
      div[data-testid="stVerticalBlockBorderWrapper"] > div {{border-radius: 17px;}}
      div[data-testid="stExpander"] {{border-radius: 14px; border-color: {palette['border']};}}
      div[data-testid="stDataFrame"] {{border-radius: 14px; overflow:hidden;}}
      .stButton > button[kind="primary"] {{background: {palette['accent']}; color: #fff;
        border-radius: 10px; border: 0; font-weight: 600;}}
      .stButton > button[kind="secondary"] {{border: 1px solid {palette['border']};
        border-radius: 10px;}}
      hr {{border-color: {palette['hr']};}}
      .ee-hero {{background: linear-gradient(125deg, {palette['hero_a']}, {palette['hero_b']});
        border: 1px solid rgba(255,255,255,.12); border-radius: 23px;
        padding: 28px 30px; margin-bottom: 22px; color:#F7FEFF;
        box-shadow: 0 18px 45px rgba(11,42,60,.12);}}
      .ee-hero h1 {{font-size: 2.03rem; font-weight: 760; line-height:1.16; letter-spacing:-.035em;
        margin: .35rem 0 .5rem; color: #fff !important;}}
      .ee-hero p {{color:#C8E0E8 !important; margin:0; font-size:.94rem;}}
      .ee-eyebrow {{font-size:.73rem; text-transform:uppercase; letter-spacing:.14em;
        color:#8BE5DB; font-weight:750;}}
      .ee-badge {{display:inline-block; background:rgba(130,229,222,.12);
        color:#A9FAEC; border:1px solid rgba(130,229,222,.25); border-radius:100px;
        padding:5px 11px; font-size:.72rem; margin-top:14px;}}
      .ee-title {{display:flex; align-items:center; justify-content:space-between;
        gap:10px; margin: 10px 0 10px;}}
      .ee-title strong {{font-size:1.08rem; letter-spacing:-.02em; color:{palette['text']};}}
      .ee-title small {{font-size:.76rem; color:{palette['muted']};}}
      .ee-item {{background: {palette['card']}; border: 1px solid {palette['border']};
        border-radius: 14px; padding: 13px 14px 8px; margin: 0 0 8px;}}
      .ee-item-head {{display:flex; justify-content:space-between; gap:8px; align-items:center;
        color:{palette['muted']}; font-size:.72rem;}}
      .ee-type {{font-size:.72rem; font-weight:750; letter-spacing:.03em;}}
      .ee-item b {{display:block; font-size:.94rem; margin:8px 0 3px;
        font-weight:650; color:{palette['text']};}}
      .ee-item p {{font-size:.77rem; color:{palette['muted']}; margin:0 0 9px;}}
      .ee-note {{background:{palette['elevated']}; color:{palette['muted']};
        border:1px solid {palette['border']}; padding:12px 15px; border-radius:12px;
        font-size:.79rem; line-height:1.55;}}
      .ee-brand {{font-size:.73rem; font-weight:800; letter-spacing:.12em;
        color:{palette['accent']}; padding-top:3px;}}
      .ee-sub {{font-size:.79rem; color:{palette['muted']}; margin-top:-3px;}}
      .ee-kicker {{font-size:.78rem; color:{palette['muted']};}}
      .ee-legend {{font-size:.8rem; color:{palette['muted']}; display:flex; gap:16px; margin-top:5px;}}
      .ee-dot {{display:inline-block; height:9px; width:9px; border-radius:50%; margin-right:5px;}}
      .ee-detail {{border:1px solid {palette['border']}; background:{palette['card']};
         border-radius:16px; padding:19px 20px; margin: 3px 0 14px;}}
      .ee-detail-grid {{display:grid; grid-template-columns:1fr 1fr; gap:13px;}}
      .ee-detail-label {{font-size:.71rem; letter-spacing:.04em; text-transform:uppercase;
        color:{palette['muted']};}}
      .ee-detail-value {{font-size:.91rem; font-weight:600; margin-top:3px; overflow-wrap:anywhere;
        color:{palette['text']};}}
      @media (max-width: 800px) {{
        .ee-hero {{padding:22px 20px;}}
        .ee-hero h1 {{font-size:1.6rem;}}
        .ee-detail-grid {{grid-template-columns:1fr;}}
      }}
    </style>
    """, unsafe_allow_html=True)
    return palette


def section_heading(name, hint=""):
    st.markdown(
        f'<div class="ee-title"><strong>{text(name)}</strong><small>{text(hint)}</small></div>',
        unsafe_allow_html=True,
    )


def show_map(events, dark: bool, height=470, detail=False):
    if not events:
        st.info("Không có sự kiện có tọa độ phù hợp bộ lọc.")
        return

    rows = []
    for event in events:
        color = [32, 183, 198, 210] if event["event_type"] == AQ else [255, 158, 87, 220]
        rows.append({
            "lat": event["centroid_latitude"],
            "lon": event["centroid_longitude"],
            "color": color,
            "label": LABELS[event["event_type"]],
            "when": date_label(event["start_time_utc"]),
            "metric": brief_metric(event),
            "id": event["event_id"],
        })
    frame = pd.DataFrame(rows)
    center_lat = float(frame["lat"].mean())
    center_lon = float(frame["lon"].mean())

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=frame,
        get_position="[lon, lat]",
        get_fill_color="color",
        get_line_color=[255, 255, 255, 230],
        get_radius=8000,  # visual marker only; NOT estimated impact radius
        radius_min_pixels=6,
        radius_max_pixels=18,
        line_width_min_pixels=1.5,
        pickable=True,
        stroked=True,
        filled=True,
    )
    style_name = "dark-matter" if dark else "positron"
    style = f"https://basemaps.cartocdn.com/gl/{style_name}-gl-style/style.json"
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(
            latitude=center_lat, longitude=center_lon,
            zoom=8.5 if detail else 5.0,
            pitch=0,
        ),
        map_style=style,
        tooltip={
            "html": "<b>{label}</b><br/>{when}<br/>{metric}",
            "style": {"backgroundColor": "#142638", "color": "#F4FBFE",
                      "fontSize": "12px", "padding": "10px", "borderRadius": "8px"},
        },
    )
    st.pydeck_chart(deck, height=height, use_container_width=True)
    st.markdown(
        '<div class="ee-legend">'
        '<span><i class="ee-dot" style="background:#20B7C6"></i>OpenAQ · CLKK</span>'
        '<span><i class="ee-dot" style="background:#FF9E57"></i>FIRMS · Điểm nóng</span>'
        '</div>',
        unsafe_allow_html=True,
    )
    st.caption("Dấu chấm là vị trí đại diện của Event, không phải ranh giới cháy hoặc bán kính tác động.")


def show_event_card(event, i):
    is_aq = event["event_type"] == AQ
    accent = TYPE_COLORS[event["event_type"]]
    st.markdown(f"""
      <div class="ee-item">
        <div class="ee-item-head">
          <span class="ee-type" style="color:{accent}">{text(SHORT_LABELS[event['event_type']])}</span>
          <span>{text(event['region_id'])}</span>
        </div>
        <b>{text(brief_metric(event))}</b>
        <p>{text(date_label(event['start_time_utc']))} · Mức độ: {text(event['severity'])}</p>
      </div>
    """, unsafe_allow_html=True)
    st.button("Xem chi tiết ↗", key=f"card_{i}_{event['event_id']}",
              on_click=open_event, args=(event["event_id"],),
              use_container_width=True)


def show_evidence(event):
    ids = event["evidence_ids"]
    if ids:
        st.dataframe(pd.DataFrame({"Evidence ID": ids}),
                     hide_index=True, use_container_width=True)
    else:
        st.info("Sự kiện chưa có evidence IDs.")
    st.caption("Event JSON chứa mã bằng chứng. Muốn xem quan trắc gốc, thời điểm và ngưỡng từng mẫu cần nối bảng curated/evidence lookup.")
    with st.expander("Xem bản ghi Event JSON nguyên gốc"):
        st.json(event)


def show_weather(event):
    weather = event.get("weather_context")
    if weather is None:
        st.info("Chưa có ngữ cảnh khí tượng ERA5 cho sự kiện này.")
        return
    a, b, c, d = st.columns(4)
    a.metric("Nhiệt độ", fmt(weather.get("temperature_c"), "°C"))
    b.metric("Gió U", fmt(weather.get("wind_u_ms"), "m/s"))
    c.metric("Gió V", fmt(weather.get("wind_v_ms"), "m/s"))
    d.metric("Mưa 1 giờ", fmt(weather.get("precipitation_mm_1h"), "mm"))
    st.caption(f"Phương pháp: {weather['context_method']}. Dữ liệu đồng thời không chứng minh quan hệ nhân quả.")


# ------------------------------ SIDEBAR -------------------------------- #

if "page" not in st.session_state:
    st.session_state["page"] = HOME
if "dark_mode" not in st.session_state:
    st.session_state["dark_mode"] = False

with st.sidebar:
    st.markdown('<div class="ee-brand">◉  ENVIRONMENTAL INTELLIGENCE</div>',
                unsafe_allow_html=True)
    st.caption("Event Monitoring · Historical Analytics")
    st.toggle("🌙 Chế độ tối", key="dark_mode")

palette = apply_theme(st.session_state["dark_mode"])
dark_mode = st.session_state["dark_mode"]

try:
    all_events = load_events()
except Exception as exc:
    st.error(f"Không thể tải Event Contract: {exc}")
    st.code(str(EVENTS_FILE))
    st.stop()

with st.sidebar:
    st.radio("Điều hướng", [HOME, DETAIL], key="page")
    st.divider()
    st.markdown("**Bộ lọc chung**")
    selected_types = st.multiselect(
        "Lớp sự kiện", options=[AQ, FIRE], default=[AQ, FIRE],
        format_func=lambda kind: LABELS[kind],
    )
    regions = sorted({e["region_id"] for e in all_events})
    selected_regions = st.multiselect("Khu vực", options=regions, default=regions)
    severities = sorted({e["severity"] for e in all_events})
    selected_severities = st.multiselect("Mức độ", options=severities, default=severities)

    if all_events:
        minimum_date = min(parse_utc(e["start_time_utc"]).date() for e in all_events)
        maximum_date = max(parse_utc(e["end_time_utc"]).date() for e in all_events)
        start_date = st.date_input("Từ ngày · UTC", value=minimum_date,
                                   min_value=minimum_date, max_value=maximum_date)
        end_date = st.date_input("Đến ngày · UTC", value=maximum_date,
                                 min_value=minimum_date, max_value=maximum_date)
    else:
        start_date = end_date = None

    if start_date is not None and start_date > end_date:
        st.error("Ngày bắt đầu phải không sau ngày kết thúc.")

    st.caption("MVP chỉ phát hiện PM2.5. Confidence n/h được lọc trước khi tạo cụm FIRMS.")
    st.divider()
    with st.expander("⚙️ Trạng thái hệ thống", expanded=False):
        st.write("**Event JSON:**", "Hợp lệ")
        st.write("**Số event trong file:**", len(all_events))
        written = datetime.fromtimestamp(EVENTS_FILE.stat().st_mtime, tz=timezone.utc)
        st.caption(f"File được sửa lúc {written:%d/%m/%Y %H:%M} UTC (không phải độ mới nguồn).")
        st.caption("Freshness nguồn, trạm thiếu dữ liệu và trạng thái Spark: chưa tích hợp metadata.")

filtered = filter_events(
    all_events, selected_types, selected_regions, selected_severities,
    start_date, end_date,
)
counts = Counter(e["event_type"] for e in filtered)


# ----------------------------- OVERVIEW -------------------------------- #

def render_overview():
    if all_events:
        first_day = min(e["start_time_utc"][:10] for e in all_events)
        last_day = max(e["start_time_utc"][:10] for e in all_events)
        period = f"{first_day}  →  {last_day} · UTC"
    else:
        period = "Chưa có sự kiện trong dữ liệu"

    st.markdown(f"""
      <div class="ee-hero">
        <div class="ee-eyebrow">Environmental Event Intelligence / Vietnam</div>
        <h1>Từ dữ liệu môi trường<br/>đến sự kiện có thể truy vết.</h1>
        <p>Bản đồ không gian–thời gian · Bất thường PM2.5 · Cụm điểm nóng vệ tinh</p>
        <span class="ee-badge">◉ DỮ LIỆU LỊCH SỬ &nbsp;·&nbsp; {text(period)}</span>
      </div>
    """, unsafe_allow_html=True)

    a, b, c, d = st.columns(4, gap="small")
    a.metric("Tổng sự kiện", len(filtered))
    b.metric("AQ · OpenAQ", counts[AQ])
    c.metric("Fire · FIRMS", counts[FIRE])
    d.metric("Độ mới dữ liệu nguồn", "Chưa có")

    st.write("")
    left, right = st.columns([2.45, 1], gap="large")
    with left:
        section_heading("Bản đồ sự kiện", "Bật / tắt lớp tại sidebar")
        # Replay slider limits map and recent-event list without fabricating live data.
        map_events = filtered
        if filtered:
            st.checkbox("Phát lại dữ liệu theo ngày", key="replay_enabled")
            if st.session_state["replay_enabled"]:
                days = sorted({parse_utc(e["start_time_utc"]).date() for e in filtered})
                chosen_day = st.select_slider("Ngày phát lại (UTC)", options=days,
                                               value=days[-1], format_func=lambda x: x.strftime("%d/%m/%Y"))
                day_end = datetime.combine(chosen_day + timedelta(days=1), time.min,
                                           tzinfo=timezone.utc)
                map_events = [e for e in filtered if parse_utc(e["start_time_utc"]) < day_end]
        show_map(map_events, dark_mode, height=490)
        section_heading("Timeline sự kiện", "Số sự kiện theo ngày bắt đầu UTC")
        if filtered:
            timeline_rows = [
                {"Ngày": e["start_time_utc"][:10], "AQ": 1 if e["event_type"] == AQ else 0,
                 "FIRMS": 1 if e["event_type"] == FIRE else 0}
                for e in filtered
            ]
            daily = pd.DataFrame(timeline_rows).groupby("Ngày", as_index=True)[["AQ", "FIRMS"]].sum()
            st.bar_chart(daily, color=[TYPE_COLORS[AQ], TYPE_COLORS[FIRE]], height=245)
        else:
            st.info("Không có dữ liệu phù hợp để vẽ timeline.")

    with right:
        section_heading("Sự kiện gần nhất", f"{len(map_events)} trong khung chọn")
        if not map_events:
            st.info("Chưa có sự kiện phù hợp.")
        else:
            for i, event in enumerate(map_events[:6]):
                show_event_card(event, i)
            if len(map_events) > 6:
                st.caption(f"Hiển thị 6/{len(map_events)} sự kiện mới nhất.")
        st.markdown(
            '<div class="ee-note">ℹ️ Các sự kiện do thuật toán phát hiện trên dữ liệu lịch sử. '
            'Điểm nóng FIRMS không đồng nghĩa với vụ cháy đã được xác nhận.</div>',
            unsafe_allow_html=True,
        )


# ----------------------------- EVENT DETAIL ---------------------------- #

def get_private_setting(name: str, default: str = "") -> str:
    """Read env first, then Streamlit secrets; never show key values in the UI."""
    env_value = os.getenv(name)
    if env_value:
        return env_value.strip()
    try:
        secret_value = st.secrets.get(name, default)
    except (FileNotFoundError, KeyError):
        return default
    return str(secret_value).strip() if secret_value is not None else default


def render_detail():
    st.button("← Trở lại bản đồ", on_click=back_to_map)
    st.markdown(f"""
      <div class="ee-hero">
        <div class="ee-eyebrow">Event investigation / Evidence first</div>
        <h1>Hồ sơ chi tiết sự kiện</h1>
        <p>Dữ liệu định lượng, ngữ cảnh môi trường và khả năng truy vết.</p>
      </div>
    """, unsafe_allow_html=True)

    if not filtered:
        st.info("Không có sự kiện phù hợp bộ lọc. Hãy quay lại sidebar để thay đổi.")
        return

    by_id = {e["event_id"]: e for e in filtered}
    if st.session_state.get("event_picker") not in by_id:
        requested = st.session_state.get("event_id")
        st.session_state["event_picker"] = requested if requested in by_id else next(iter(by_id))

    selected_id = st.selectbox(
        "Chọn sự kiện cần phân tích", options=list(by_id), key="event_picker",
        format_func=lambda e_id: (
            f"{LABELS[by_id[e_id]['event_type']]} · {date_label(by_id[e_id]['start_time_utc'])} · {e_id[:20]}"
        ),
    )
    event = by_id[selected_id]
    m = event["metrics"]

    section_heading("01 / Thông tin sự kiện", "Contract-compliant event")
    meta = [
        ("Loại sự kiện", LABELS[event["event_type"]]),
        ("Khu vực", event["region_id"]),
        ("Bắt đầu", date_label(event["start_time_utc"])),
        ("Kết thúc", date_label(event["end_time_utc"])),
        ("Mức độ", event["severity"]),
        ("Thuật toán", event["algorithm_version"]),
    ]
    cells = "".join(
        f'<div><div class="ee-detail-label">{text(k)}</div>'
        f'<div class="ee-detail-value">{text(v)}</div></div>' for k, v in meta
    )
    st.markdown(f"""
        <div class="ee-detail">
          <div class="ee-detail-label">EVENT ID</div>
          <div class="ee-detail-value">{text(event['event_id'])}</div>
          <hr/>
          <div class="ee-detail-grid">{cells}</div>
        </div>
    """, unsafe_allow_html=True)

    section_heading("02 / Chỉ số phát hiện", "Giá trị tổng hợp từ thuật toán")
    if event["event_type"] == AQ:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Peak PM2.5", fmt(m["peak_ugm3"], "µg/m³"))
        c2.metric("Mean PM2.5", fmt(m["mean_ugm3"], "µg/m³"))
        c3.metric("Baseline", fmt(m["baseline_ugm3"], "µg/m³"))
        c4.metric("Thời lượng", fmt(m["duration_hours"], "giờ"))
        st.markdown("**So sánh peak và baseline lịch sử**")
        comparison = pd.DataFrame(
            {"PM2.5 (µg/m³)": [m["baseline_ugm3"], m["peak_ugm3"]]},
            index=["Baseline", "Peak"],
        )
        st.bar_chart(comparison, color=TYPE_COLORS[AQ], height=230)
        st.caption("Đây là hai giá trị tổng hợp của Event, không phải chuỗi đo PM2.5 theo giờ.")
        st.write("**Sensor IDs:**", ", ".join(m["sensor_ids"]))
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Hotspots", m["hotspot_count"])
        c2.metric("Max FRP", fmt(m["max_frp_mw"], "MW"))
        c3.metric("Tổng FRP", fmt(m["sum_frp_mw"], "MW"))
        st.write("**Vệ tinh:**", ", ".join(m["satellites"]) or "Chưa có")
        st.caption("Cụm được tạo bằng ST-DBSCAN. FRP có thể thiếu; giá trị null không được hiểu là 0.")

    st.write("")
    left, right = st.columns([1.4, 1], gap="large")
    with left:
        section_heading("03 / Vị trí đại diện", "Không suy ra vùng ảnh hưởng")
        show_map([event], dark_mode, height=360, detail=True)
    with right:
        section_heading("04 / Ngữ cảnh ERA5", "Chỉ hiện khi có dữ liệu")
        weather = event.get("weather_context")
        if weather is None:
            st.info("Chưa có weather_context trong Event JSON.")
        else:
            st.write("**Nhiệt độ:**", fmt(weather.get("temperature_c"), "°C"))
            st.write("**Gió U:**", fmt(weather.get("wind_u_ms"), "m/s"))
            st.write("**Gió V:**", fmt(weather.get("wind_v_ms"), "m/s"))
            st.write("**Mưa 1 giờ:**", fmt(weather.get("precipitation_mm_1h"), "mm"))
            st.caption(f"Phương pháp: {weather['context_method']}. Đồng thời không chứng minh nguyên nhân.")

    section_heading("05 / Diễn giải & bằng chứng", "LLM không thay thế phát hiện định lượng")
    info, evidence = st.columns([1, 1.55], gap="large")
    with info:
        st.markdown("**Panel diễn giải tự động**")
        gemini_key = get_private_setting("GEMINI_API_KEY")
        groq_key = get_private_setting("GROQ_API_KEY")
        gemini_model = get_private_setting("GEMINI_MODEL", DEFAULT_GEMINI_MODEL)
        groq_model = get_private_setting("GROQ_MODEL", DEFAULT_GROQ_MODEL)

        st.caption(f"Chính: Gemini (`{gemini_model}`) · Dự phòng: Groq (`{groq_model}`)")
        fallback_enabled = st.checkbox(
            "Cho phép tự động chuyển sang Groq khi Gemini bị giới hạn/lỗi tạm thời",
            value=True,
            key="llm_allow_groq_fallback",
        )
        st.caption("Khi chuyển dự phòng, cùng dữ liệu sự kiện sẽ được gửi sang Groq. "
                   "Hãy tắt tùy chọn nếu không muốn chia sẻ với nhà cung cấp thứ hai.")

        if not gemini_key:
            st.warning("Chưa cấu hình GEMINI_API_KEY trong .streamlit/secrets.toml hoặc biến môi trường. Dashboard vẫn chạy bình thường.")
        elif fallback_enabled and not groq_key:
            st.info("Chưa cấu hình GROQ_API_KEY. Gemini vẫn chạy, nhưng không có dự phòng.")

        cache_key = (event_fingerprint(event), gemini_model, groq_model, fallback_enabled)
        if "llm_explanations" not in st.session_state:
            st.session_state["llm_explanations"] = {}

        if st.button(
            "✦ Diễn giải tự động bằng LLM",
            type="primary",
            disabled=not bool(gemini_key),
            use_container_width=True,
            key="explain_event_gemini_groq",
        ):
            if cache_key in st.session_state["llm_explanations"]:
                st.info("Đang dùng diễn giải đã tạo trong phiên này; không gọi API lại. Xóa kết quả nếu muốn tạo mới.")
            else:
                try:
                    with st.spinner("Đang tổng hợp diễn giải từ dữ liệu Event..."):
                        result = explain_event(
                            event,
                            gemini_api_key=gemini_key,
                            groq_api_key=groq_key,
                            gemini_model=gemini_model,
                            groq_model=groq_model,
                            allow_fallback=fallback_enabled,
                        )
                    st.session_state["llm_explanations"][cache_key] = result
                except LLMServiceError as exc:
                    st.error(str(exc))
                except Exception:
                    st.error("Không thể diễn giải sự kiện. Kiểm tra model, mạng và API keys.")

        result = st.session_state["llm_explanations"].get(cache_key)
        if result is not None:
            st.caption(f"Đã diễn giải bằng **{result.provider} · {result.model}**"
                       + (" (chuyển dự phòng)" if result.used_fallback else ""))
            st.markdown(result.text)
            if st.button("Xóa diễn giải trong phiên này", key="clear_llm_result"):
                st.session_state["llm_explanations"].pop(cache_key, None)
                st.rerun()
        else:
            st.info("Chưa có diễn giải. Hãy bấm nút để gọi LLM khi cần.")

        with st.expander("Xem dữ liệu sẽ gửi tới nhà cung cấp LLM"):
            st.code(build_prompt(event), language="json")
        st.caption("⚠️ AI có thể sai. Không dùng diễn giải làm bằng chứng độc lập, "
                   "khẳng định nguyên nhân hay cảnh báo chính thức.")
    with evidence:
        st.markdown("**Bằng chứng quan trắc**")
        show_evidence(event)


if st.session_state["page"] == HOME:
    render_overview()
else:
    render_detail()

st.write("")
st.divider()
st.caption(
    "Nguồn: OpenAQ · NASA FIRMS · ERA5/Copernicus (khi có ngữ cảnh). "
    "Dữ liệu lịch sử, không phải cảnh báo thời gian thực. "
    "Hotspot không tương đương cháy đã xác minh; LLM không phải bằng chứng xác định nguyên nhân."
)
