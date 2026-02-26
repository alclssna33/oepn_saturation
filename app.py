"""
개원포화도 분석 대시보드
streamlit run app.py
"""

import json
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from config import ADMIN_PASSWORD
from modules.data_merge import (
    DataMerger,
    calc_saturation_index,
    merge_with_population,
)
from modules.hospital_api import HIRA_SIDO_CODES, SPECIALTY_CODES as _SP_ALL
from modules.population_api import SIDO_CODES, PopulationAPIClient

# ══════════════════════════════════════════════════════════════════════════════
# 상수 및 설정
# ══════════════════════════════════════════════════════════════════════════════

_NATIONAL_GEOJSON = ROOT / "data" / "geojson" / "national_dong.geojson"
_SEOUL_GEOJSON    = ROOT / "data" / "geojson" / "seoul_dong.geojson"
GEOJSON_PATH = _NATIONAL_GEOJSON if _NATIONAL_GEOJSON.exists() else _SEOUL_GEOJSON

SPECIALTY_SELECT: dict[str, str] = {
    # ── 의과 ─────────────────────────────────────────────────────────────────
    "내과":              "01", "신경과":              "02", "정신건강의학과":    "03",
    "외과":              "04", "정형외과":            "05", "신경외과":          "06",
    "흉부외과":          "07", "성형외과":            "08", "마취통증의학과":    "09",
    "산부인과":          "10", "소아청소년과":        "11", "안과":              "12",
    "이비인후과":        "13", "피부과":              "14", "비뇨의학과":        "15",
    "영상의학과":        "16", "재활의학과":          "21", "가정의학과":        "23",
    "응급의학과":        "24", "직업환경의학과":      "25",
    # ── 치과 ─────────────────────────────────────────────────────────────────
    "치과":              "49", "치과교정과":          "52", "소아치과":          "53",
    "치주과":            "54", "치과보존과":          "55", "통합치의학과":      "61",
}

LEVEL_COLOR = {
    "포화": "#DC2626", "보통": "#D97706", "여유": "#16A34A", "데이터없음": "#9CA3AF",
}

st.set_page_config(page_title="개원포화도 분석", page_icon="🏥", layout="wide", initial_sidebar_state="expanded")

# ══════════════════════════════════════════════════════════════════════════════
# CSS (아이콘 보정 포함)
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"], [class*="st-"] { font-family: 'Noto Sans KR', sans-serif !important; color: #1F2937; }
section[data-testid="stSidebar"] > div { background: #F9FAFB; padding-top: 1.2rem; }
[data-testid="stMetric"] { background: #FFFFFF; border: 1.5px solid #E5E7EB; border-radius: 14px; padding: 18px 22px !important; }
.chart-card { background: #FFFFFF; border: 1.5px solid #E5E7EB; border-radius: 14px; padding: 16px 20px; margin-bottom: 12px; }
.chart-title { font-size: 13px; font-weight: 600; color: #374151; margin-bottom: 10px; border-left: 3px solid #2563EB; padding-left: 8px; }
@import url('https://fonts.googleapis.com/variablefonts/materialsymbolsoutlined');
.material-symbols-outlined, [data-testid="stIconMaterial"] { font-family: 'Material Symbols Outlined' !important; -webkit-font-feature-settings: 'liga'; }
/* ── 초록 계열 버튼 테마 ── */
[data-testid="stBaseButton-primary"],
[data-testid="stBaseButton-primary"] * {
    background-color: #16A34A !important;
    border-color: #16A34A !important;
    color: #FFFFFF !important;
}
[data-testid="stBaseButton-primary"]:hover,
[data-testid="stBaseButton-primary"]:hover * {
    background-color: #15803D !important;
    border-color: #15803D !important;
    color: #FFFFFF !important;
}
[data-testid="stBaseButton-primary"]:active,
[data-testid="stBaseButton-primary"]:active * {
    background-color: #166534 !important;
    border-color: #166534 !important;
    color: #FFFFFF !important;
}
/* multiselect 선택 태그 */
[data-testid="stMultiSelect"] [data-baseweb="tag"] {
    background-color: #16A34A !important;
}
/* link button (네이버 지도 등) */
[data-testid="stLinkButton"] a,
[data-testid="stLinkButton"] a * {
    background-color: #16A34A !important;
    border-color: #16A34A !important;
    color: #FFFFFF !important;
}
[data-testid="stLinkButton"] a:hover,
[data-testid="stLinkButton"] a:hover * {
    background-color: #15803D !important;
    border-color: #15803D !important;
    color: #FFFFFF !important;
}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# 헬퍼 함수
# ══════════════════════════════════════════════════════════════════════════════

def _standardize_code(code: str) -> str:
    if not code: return code
    s = str(code)
    if s.startswith("41000"): return "36" + s[5:]
    if s.startswith("42"): return "51" + s[2:]
    if s.startswith("45"): return "52" + s[2:]
    return s

@st.cache_resource
def _load_geojson() -> dict:
    with open(GEOJSON_PATH, encoding="utf-8") as f: gj = json.load(f)
    for f in gj["features"]:
        f["properties"]["adm_cd2"] = _standardize_code(f["properties"].get("adm_cd2"))
    return gj

@st.cache_data(ttl=86400, show_spinner=False)
def _get_sgg_options(sido_name: str) -> dict[str, str]:
    client = PopulationAPIClient()
    sido_cd = SIDO_CODES.get(sido_name, "")
    if not sido_cd: return {}
    df = client.get_sgg_list(sido_cd)
    return dict(zip(df["sggNm"], df["admmCd"])) if not df.empty else {}

@st.cache_data(ttl=3600, show_spinner=False)
def _load_data(sgg_cd_pop, hira_sido_cd, sgg_name, specialty_codes, year_month, num_col, den_col, analysis_level="dong", cl_codes=None) -> dict:
    merger = DataMerger(GEOJSON_PATH)
    res = merger.run(sgg_cd_pop=sgg_cd_pop, hira_sido_cd=hira_sido_cd, sgg_name=sgg_name, specialty_codes=list(specialty_codes),
                     year_month=year_month, num_col=num_col, den_col=den_col, analysis_level=analysis_level, cl_codes=list(cl_codes) if cl_codes else None)

    if res["population"].empty:
        try:
            curr_dt = datetime.strptime(year_month, "%Y%m")
            prev_month = (curr_dt.replace(day=1) - timedelta(days=1)).strftime("%Y%m")
            res = merger.run(sgg_cd_pop=sgg_cd_pop, hira_sido_cd=hira_sido_cd, sgg_name=sgg_name, specialty_codes=list(specialty_codes),
                             year_month=prev_month, num_col=num_col, den_col=den_col, analysis_level=analysis_level, cl_codes=list(cl_codes) if cl_codes else None)
            res["used_year_month"] = prev_month
        except: pass
    else: res["used_year_month"] = year_month
    
    if analysis_level in ["national", "sido"]:
        gdf = gpd.read_file(GEOJSON_PATH)
        gdf["adm_cd2"] = gdf["adm_cd2"].apply(_standardize_code)
        if analysis_level == "national": gdf["dissolve_key"] = gdf["adm_cd2"].str[:2]
        else:
            prefix = sgg_cd_pop[:2]
            sgg_codes = res.get("sgg_codes", set())
            def _mk(adm_cd: str) -> str:
                c = adm_cd[:4] + "0"
                return c if c in sgg_codes else adm_cd[:5]
            gdf["dissolve_key"] = gdf["adm_cd2"].apply(_mk)
            gdf = gdf[gdf["dissolve_key"].str.startswith(prefix)].copy()
        dissolved = gdf.dissolve(by="dissolve_key").reset_index()
        dissolved["adm_cd2"] = dissolved["dissolve_key"]
        res["geojson_dissolved"] = json.loads(dissolved.to_json())
    return res

def _make_choropleth(si_df: pd.DataFrame, geojson: dict) -> go.Figure:
    loc_col = "match_key" if "match_key" in si_df.columns else "admmCd"
    codes = set(si_df[loc_col].dropna().astype(str))
    gj_filtered = {"type": "FeatureCollection", "features": [f for f in geojson["features"] if f["properties"].get("adm_cd2") in codes]}

    # saturation_level → 이산 z값 (막대그래프와 동일한 색 체계)
    _LEVEL_Z = {"포화": 0, "보통": 1, "여유": 2, "데이터없음": 3}
    _DISCRETE_CS = [
        [0.000, "#DC2626"], [0.249, "#DC2626"],   # 포화  — 빨강
        [0.250, "#D97706"], [0.499, "#D97706"],   # 보통  — 주황
        [0.500, "#16A34A"], [0.749, "#16A34A"],   # 여유  — 초록
        [0.750, "#9CA3AF"], [1.000, "#9CA3AF"],   # 데이터없음 — 회색
    ]

    df = si_df.copy()
    df["_z"] = df["saturation_level"].map(_LEVEL_Z).fillna(3)
    df["_hover_si"] = df.apply(lambda row: "기회 최대" if row["clinic_count"] == 0 and row["총인구수"] > 0
                               else (f"{row['SI_normalized']:.2f}" if pd.notna(row['SI_normalized']) else "데이터없음"), axis=1)
    df["_hover_pop"] = df["총인구수"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "N/A") if "총인구수" in df.columns else "N/A"
    df["_hover_hh"]  = df["세대수"].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "N/A") if "세대수" in df.columns else "N/A"

    fig = go.Figure(go.Choroplethmapbox(
        geojson=gj_filtered, featureidkey="properties.adm_cd2",
        locations=df[loc_col].astype(str), z=df["_z"],
        colorscale=_DISCRETE_CS,
        zmin=0, zmax=3,
        colorbar=dict(
            tickvals=[0.375, 1.125, 1.875, 2.625],
            ticktext=["포화", "보통", "여유", "데이터없음"],
            title="등급", thickness=14, len=0.5,
        ),
        marker_opacity=0.78, marker_line_width=1.2, marker_line_color="#FFFFFF",
        customdata=df[["행정동명", "_hover_si", "saturation_level", "clinic_count", "specialist_count", "_hover_pop", "_hover_hh"]].values,
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            "포화도 지수: %{customdata[1]}&nbsp;&nbsp;등급: %{customdata[2]}<br>"
            "의원 수: %{customdata[3]}개&nbsp;&nbsp;전문의 수: %{customdata[4]}명<br>"
            "총 인구수: %{customdata[5]}명&nbsp;&nbsp;세대수: %{customdata[6]}세대"
            "<extra></extra>"
        ),
    ))

    try:
        gdf_tmp = gpd.GeoDataFrame.from_features(gj_filtered["features"])
        c = gdf_tmp.geometry.centroid
        lat, lon = float(c.y.mean()), float(c.x.mean())
    except:
        lat, lon = 36.5, 127.5

    fig.update_layout(
        mapbox=dict(style="carto-positron", zoom=10, center=dict(lat=lat, lon=lon), uirevision="map-view"),
        margin=dict(r=0, t=0, l=0, b=0),
        height=530,
        clickmode="event+select",
    )
    return fig

def _make_bar_chart(si_df: pd.DataFrame, si_col: str = "SI_normalized") -> go.Figure:
    df = si_df.dropna(subset=[si_col]).copy()
    # 기회 최대(∞→3.0) 구분 표시용으로 원본 보존, 정렬은 정상값 기준
    df_inf = df[df[si_col] == 3.0]
    df_normal = df[df[si_col] != 3.0].sort_values(si_col, ascending=True)
    df = pd.concat([df_inf, df_normal])  # 기회 최대 지역은 맨 위

    name_col = "행정동명" if "행정동명" in df.columns else ("시군구명" if "시군구명" in df.columns else "시도명")
    bar_h = max(min(len(df) * 26, 700), 300)

    fig = go.Figure(go.Bar(
        x=df[si_col].clip(upper=2.5), y=df[name_col], orientation="h",
        marker_color=[LEVEL_COLOR.get(lvl, "#9CA3AF") for lvl in df["saturation_level"]],
        text=df[si_col].map(lambda x: "∞" if x == 3.0 else f"{x:.2f}"),
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>포화도: %{text}<extra></extra>",
    ))
    fig.update_layout(
        height=bar_h,
        margin=dict(r=50, t=10, l=10, b=20),
        plot_bgcolor="white",
        showlegend=False,
        xaxis=dict(showgrid=True, gridcolor="#F3F4F6", title="포화도 지수"),
        yaxis=dict(tickfont=dict(size=11)),
        shapes=[dict(
            type="line", x0=1, x1=1, y0=0, y1=1, yref="paper",
            line=dict(color="#374151", width=1.5, dash="dot"),
        )],
        annotations=[dict(
            x=1, y=1, yref="paper", xanchor="left", yanchor="top",
            text="  평균(1.0)", showarrow=False,
            font=dict(size=11, color="#374151"),
        )],
    )
    return fig

# ══════════════════════════════════════════════════════════════════════════════
# 병원 상세 팝업
# ══════════════════════════════════════════════════════════════════════════════

@st.dialog("🏥 병원 상세 정보", width="large")
def _show_hospital_detail(hosp: pd.Series, all_hosp_df: pd.DataFrame) -> None:
    name    = str(hosp.get("yadmNm", "") or "")
    addr    = str(hosp.get("addr", "") or "")
    cl_nm   = str(hosp.get("clCdNm", "") or "")
    ykiho   = str(hosp.get("ykiho", "") or "")
    dr_tot  = int(hosp.get("drTotCnt", 0) or 0)
    sdr_cnt = int(hosp.get("mdeptSdrCnt", 0) or 0)
    estb    = str(hosp.get("estbDd", "") or "").strip()

    # Pandas SQLite 저장 시 '20140102 00:00:00' 형태로 들어갈 수 있음
    estb_clean = estb.split()[0].replace("-", "") if estb else ""
    estb_fmt = f"{estb_clean[:4]}-{estb_clean[4:6]}-{estb_clean[6:8]}" if len(estb_clean) >= 8 and estb_clean[:8].isdigit() else "정보 없음"

    # 같은 ykiho를 가진 모든 행에서 진료과목 수집
    if ykiho and not all_hosp_df.empty and "ykiho" in all_hosp_df.columns:
        specialties = sorted(
            all_hosp_df[all_hosp_df["ykiho"] == ykiho]["specialty_nm"].dropna().unique().tolist()
        )
    else:
        sp_fallback = str(hosp.get("specialty_nm", "") or "")
        specialties = [sp_fallback] if sp_fallback else []

    st.markdown(f"### {name}")
    st.caption(f"{cl_nm}　|　{addr}")
    st.divider()

    st.markdown("**진료과목**")
    if specialties:
        badges = " ".join([
            f'<span style="background:#DCFCE7;color:#166534;padding:3px 12px;'
            f'border-radius:14px;font-size:13px;font-weight:500;margin:2px 2px 4px;display:inline-block">{s}</span>'
            for s in specialties
        ])
        st.markdown(badges, unsafe_allow_html=True)
    else:
        st.write("정보 없음")

    st.divider()

    c1, c2, c3 = st.columns(3)
    c1.metric("의사 총수", f"{dr_tot}명")
    c2.metric("전문의 수", f"{sdr_cnt}명")
    c3.metric("개설일자", estb_fmt)

    st.divider()
    addr = str(hosp.get("addr", "") or "")
    # 주소 텍스트에서 강진구, 광진구 등 2번째 단어(시군구명) 추출
    parts = addr.split()
    sgg_nm = parts[1] if len(parts) > 1 else ""
    naver_url = f"https://map.naver.com/v5/search/{quote(f'{name} {sgg_nm}'.strip())}"
    _, btn_col, _ = st.columns([1, 2, 1])
    with btn_col:
        st.link_button("📍 네이버 지도에서 보기", naver_url, use_container_width=True, type="primary")

# ══════════════════════════════════════════════════════════════════════════════
# 사이드바
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## 🏥 개원포화도")
    st.divider()
    sido_options = ["전국"] + list(SIDO_CODES.keys())
    sido_name = st.selectbox("시도", sido_options, index=1)
    
    sgg_name, sgg_cd_pop, hira_sido, analysis_level, admin_pw_input = "전체", "0000000000", "", "national", ""
    if sido_name != "전국":
        with st.spinner("로딩 중..."): sgg_opts = _get_sgg_options(sido_name)
        if not sgg_opts:
            sgg_cd_pop, hira_sido, analysis_level = SIDO_CODES[sido_name], HIRA_SIDO_CODES.get(sido_name, ""), "sido"
        else:
            sgg_all = {"전체": SIDO_CODES[sido_name]}; sgg_all.update(sgg_opts)
            sgg_list = list(sgg_all.keys())
            sgg_name = st.selectbox("시군구", sgg_list, index=sgg_list.index("종로구") if "종로구" in sgg_list else 0)
            sgg_cd_pop, hira_sido = sgg_all[sgg_name], HIRA_SIDO_CODES.get(sido_name, "")
            analysis_level = "sido" if sgg_name == "전체" else "dong"
    else:
        admin_pw_input = st.text_input("관리자 비밀번호", type="password")

    selected_sp_names = st.multiselect("진료과목", options=list(SPECIALTY_SELECT.keys()), default=["내과"])
    
    st.markdown("##### 📊 분석 기준")
    denom_type = st.radio("분모", ["의원 수", "전문의 수"], horizontal=True)
    denom_col = "clinic_count" if denom_type == "의원 수" else "specialist_count"
    num_opts = {"총 인구수": "총인구수", "세대수": "세대수", "20세 이하": "20세이하인구", "20~40세": "20_40세인구", "40~60세": "40_60세인구", "60세 이상": "60세이상인구"}
    si_mode_label = st.selectbox("분자 (대상 인구)", list(num_opts.keys()), index=0)
    num_col = num_opts[si_mode_label]

    _prev = datetime.now().replace(day=1) - timedelta(days=1)
    year_month = st.text_input("기준 연월", value=_prev.strftime("%Y%m"))
    cl_opts = st.multiselect("의료기관 종류", options=["의원 (31)", "병원 (21)", "종합병원 (11)"], default=["의원 (31)"])
    cl_codes = tuple(x.split("(")[1].rstrip(")").strip() for x in cl_opts) or ("31",)

    run_btn = st.button("🔍  분석 실행", use_container_width=True, type="primary")
    if st.button("🗑️ 캐시 초기화", use_container_width=True):
        st.cache_data.clear(); st.session_state.clear(); st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════════════════════

if run_btn:
    if analysis_level == "national" and admin_pw_input != ADMIN_PASSWORD:
        st.error("❌ 전국 분석 권한이 없습니다."); st.stop()
    
    sp_codes = tuple(SPECIALTY_SELECT[nm] for nm in selected_sp_names)
    _LOADING_IMG = "https://lh3.googleusercontent.com/d/1LcNs3lhy8907rWmyRfh_ZcFQdPuF7Spq"
    loading_slot = st.empty()
    loading_slot.markdown(f"""
<style>
@keyframes _kl_spin {{
    0%   {{ transform: rotate(0deg); }}
    100% {{ transform: rotate(360deg); }}
}}
.kl-ring {{
    width: 60px; height: 60px;
    border: 6px solid #E5E7EB;
    border-top-color: #2563EB;
    border-radius: 50%;
    animation: _kl_spin 0.8s linear infinite;
    margin: 0 auto;
}}
</style>
<div style="display:flex; flex-direction:column; align-items:center; justify-content:center;
            padding: 64px 20px; gap: 22px;">
  <img src="{_LOADING_IMG}" style="width:200px; border-radius:18px; box-shadow:0 4px 20px rgba(0,0,0,0.12);" />
  <div class="kl-ring"></div>
  <div style="text-align:center;">
    <p style="margin:0 0 6px; font-size:18px; font-weight:700; color:#1F2937;">데이터 분석 중...</p>
    <p style="margin:0; font-size:13px; color:#9CA3AF;">인구 및 병의원 데이터를 수집하고 있습니다</p>
  </div>
</div>
""", unsafe_allow_html=True)

    try:
        results = _load_data(sgg_cd_pop, hira_sido, sgg_name, sp_codes, year_month, num_col, denom_col, analysis_level, cl_codes)
        st.session_state.update({"results": results, "sp_names": selected_sp_names, "sido_name": sido_name, "sgg_name": sgg_name, "analysis_level": analysis_level})
    except Exception as e:
        st.error(f"⚠️ 분석 오류: {e}")
        with st.expander("🚨 상세 오류 로그 (개발자 확인용)"):
            st.code(traceback.format_exc())
    loading_slot.empty()

if "results" in st.session_state:
    res = st.session_state["results"]
    pop_df     = res["population"]
    hosp_summary = res["hospital_summary"]
    hosp_df    = res.get("hospitals", pd.DataFrame())
    geojson    = res.get("geojson_dissolved", _load_geojson())
    sp_names   = st.session_state["sp_names"]

    # ── 디버그 (기본 접힘) ─────────────────────────────────────────────

    tabs = st.tabs(sp_names)
    for tab, sp_nm in zip(tabs, sp_names):
        with tab:
            sp_cd  = SPECIALTY_SELECT[sp_nm]
            si_df  = res["saturation"].get(sp_cd)
            sel_key = f"dong_sel_{sp_nm}"

            if si_df is None or si_df.empty:
                st.info("해당 과목 데이터가 없습니다.")
                continue

            # ── 요약 지표 ─────────────────────────────────────────────
            total_clinics = int(si_df["clinic_count"].sum())
            valid_si = si_df[(si_df["SI_normalized"] != 3.0) & si_df["SI_normalized"].notna()]["SI_normalized"]
            avg_si    = valid_si.mean() if not valid_si.empty else float("nan")
            blue_ocean = int((si_df["clinic_count"] == 0).sum())
            saturated  = int((si_df["saturation_level"] == "포화").sum())

            mc1, mc2, mc3, mc4 = st.columns(4)
            mc1.metric("총 의원 수",        f"{total_clinics:,}개")
            mc2.metric("분석 행정동",        f"{len(si_df):,}개")
            mc3.metric("평균 포화도",        f"{avg_si:.2f}" if not pd.isna(avg_si) else "N/A")
            mc4.metric("기회 지역 (의원 0)", f"{blue_ocean:,}개")

            st.markdown("---")

            # ── 지도 + 막대그래프 ─────────────────────────────────────
            col_map, col_bar = st.columns([6, 4], gap="medium")

            with col_map:
                st.markdown('<p class="chart-title">📍 행정동별 포화도 지도 — 클릭하면 의원 목록 표시</p>', unsafe_allow_html=True)
                map_event = st.plotly_chart(
                    _make_choropleth(si_df, geojson),
                    use_container_width=True,
                    on_select="rerun",
                    key=f"map_{sp_nm}",
                    config={
                        "scrollZoom": True,
                        "displayModeBar": True,
                        "displaylogo": False,
                        "modeBarButtonsToRemove": ["toImage", "lasso2d", "select2d"],
                    },
                )
                # 선택된 지역 session_state 저장
                if (map_event and hasattr(map_event, "selection")
                        and map_event.selection.points):
                    st.session_state[sel_key] = str(
                        map_event.selection.points[0].get("location", "")
                    )

            with col_bar:
                st.markdown('<p class="chart-title">📊 포화도 순위 — 위로 갈수록 기회 많음</p>', unsafe_allow_html=True)
                st.plotly_chart(
                    _make_bar_chart(si_df),
                    use_container_width=True,
                    key=f"bar_{sp_nm}",
                )

            # ── 클릭된 행정동 의원 목록 ───────────────────────────────
            selected_key = st.session_state.get(sel_key, "")
            if selected_key:
                mask     = si_df["match_key"].astype(str) == selected_key
                dong_row = si_df[mask]
                dong_name = (dong_row["행정동명"].values[0]
                             if not dong_row.empty and "행정동명" in dong_row.columns
                             else selected_key)
                level    = dong_row["saturation_level"].values[0] if not dong_row.empty else ""
                n_clinic = int(dong_row["clinic_count"].values[0])   if not dong_row.empty else 0
                n_spec   = int(dong_row["specialist_count"].values[0]) if not dong_row.empty else 0
                si_val   = dong_row["SI_normalized"].values[0] if not dong_row.empty else None
                n_pop    = int(dong_row["총인구수"].values[0]) if not dong_row.empty and "총인구수" in dong_row.columns else 0
                n_hh     = int(dong_row["세대수"].values[0])  if not dong_row.empty and "세대수"  in dong_row.columns else 0

                st.divider()
                hdr_col, close_col = st.columns([9, 1])
                with hdr_col:
                    badge_color = LEVEL_COLOR.get(level, "#9CA3AF")
                    st.markdown(
                        f'<h4 style="margin:0;line-height:2">📋 {dong_name} &nbsp;'
                        f'<span style="background:{badge_color};color:white;padding:3px 14px;'
                        f'border-radius:20px;font-size:13px;font-weight:600">{level}</span></h4>',
                        unsafe_allow_html=True,
                    )
                with close_col:
                    if st.button("✕ 닫기", key=f"close_{sp_nm}"):
                        st.session_state.pop(sel_key, None)
                        st.rerun()

                dm1, dm2, dm3, dm4, dm5 = st.columns(5)
                dm1.metric(f"{sp_nm} 의원 수", f"{n_clinic}개")
                dm2.metric("전문의 수", f"{n_spec}명")
                si_label = ("기회 최대" if (n_clinic == 0 or si_val == 3.0)
                            else (f"{si_val:.2f}" if pd.notna(si_val) else "N/A"))
                dm3.metric("포화도 지수", si_label)
                dm4.metric("총 인구수", f"{n_pop:,}명")
                dm5.metric("세대수", f"{n_hh:,}세대")

                if not hosp_df.empty and "match_key" in hosp_df.columns and "specialty_cd" in hosp_df.columns:
                    clinics = hosp_df[
                        (hosp_df["match_key"].astype(str) == selected_key) &
                        (hosp_df["specialty_cd"] == sp_cd)
                    ].copy().reset_index(drop=True)
                    if clinics.empty:
                        st.info(f"해당 행정동에 {sp_nm} 의원이 없거나, 좌표 미등록으로 지도에 매핑되지 않았습니다.")
                    else:
                        # 헤더 행
                        h = st.columns([3, 1.5, 4, 0.8, 0.8])
                        for txt, col in zip(["의원명", "종별", "주소", "전문의", ""], h):
                            col.markdown(f"<span style='font-size:11px;font-weight:600;color:#6B7280'>{txt}</span>", unsafe_allow_html=True)
                        st.markdown("<hr style='margin:2px 0 6px;border-color:#E5E7EB'>", unsafe_allow_html=True)
                        # 데이터 행
                        for idx, row in clinics.iterrows():
                            r = st.columns([3, 1.5, 4, 0.8, 0.8])
                            r[0].write(row.get("yadmNm", ""))
                            r[1].write(row.get("clCdNm", ""))
                            r[2].write(row.get("addr", ""))
                            r[3].write(f"{int(row.get('mdeptSdrCnt', 0))}명")
                            if r[4].button("상세", key=f"det_{sp_cd}_{idx}"):
                                _show_hospital_detail(row, hosp_df)
                else:
                    st.info("병원 위치 데이터가 없습니다.")
