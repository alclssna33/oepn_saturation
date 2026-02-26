"""
동 단위 데이터 병합 및 포화도 지수 산출 모듈
"""

import os
import warnings
import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

warnings.filterwarnings("ignore", category=UserWarning)

_NATIONAL_GEOJSON = Path(__file__).parent.parent / "data" / "geojson" / "national_dong.geojson"
_SEOUL_GEOJSON    = Path(__file__).parent.parent / "data" / "geojson" / "seoul_dong.geojson"
_DEFAULT_GEOJSON  = _NATIONAL_GEOJSON if _NATIONAL_GEOJSON.exists() else _SEOUL_GEOJSON

def _get_hira_to_pop_map():
    from modules.hospital_api import HIRA_SGG_MAP
    return {str(v): str(k) for k, v in HIRA_SGG_MAP.items()}

SATURATION_LEVELS = {
    "포화": (0.0, 0.8),
    "보통": (0.8, 1.2),
    "여유": (1.2, float("inf")),
}

# ── 헬퍼: 안전한 값 추출 ──
def _safe_val(row, col):
    """중복 컬럼명이 있어도 안전하게 첫 번째 값을 반환"""
    val = row[col]
    if hasattr(val, "__iter__") and not isinstance(val, (str, bytes)):
        return val.iloc[0] if hasattr(val, "iloc") else val[0]
    return val

# ── 병합 및 지수 산출 함수 ──────────────────────────────────────────────────────
def merge_with_population(pop_df: pd.DataFrame, hospital_summary: pd.DataFrame, specialty_cd: str) -> pd.DataFrame:
    if hospital_summary.empty or "specialty_cd" not in hospital_summary.columns:
        hs = pd.DataFrame(columns=["match_key", "clinic_count", "specialist_count"])
    else:
        hs = hospital_summary[hospital_summary["specialty_cd"] == specialty_cd].copy()
    
    pop_df["match_key"] = pop_df["match_key"].astype(str)
    hs["match_key"] = hs["match_key"].astype(str)
    merged = pop_df.merge(hs[["match_key", "clinic_count", "specialist_count"]], on="match_key", how="left")
    for col in ["clinic_count", "specialist_count"]:
        merged[col] = pd.to_numeric(merged[col], errors='coerce').fillna(0).astype(int)
    return merged

def calc_saturation_index(merged_df: pd.DataFrame, num_col: str, den_col: str) -> pd.DataFrame:
    df = merged_df.copy()
    if num_col not in df.columns:
        df["SI_normalized"] = float("nan"); df["saturation_level"] = "데이터없음"; return df
    if den_col not in df.columns: df[den_col] = 0
    n, d = df[num_col].astype(float), df[den_col].astype(float)
    df["SI_raw"] = n / d.replace(0, float("nan"))
    df.loc[(d == 0) & (n > 0), "SI_raw"] = float("inf")
    df.loc[n == 0, "SI_raw"] = 0.0
    valid_si = df[df["SI_raw"] != float("inf")]["SI_raw"].dropna()
    mean_si = valid_si.mean() if not valid_si.empty else 1.0
    df["SI_normalized"] = df["SI_raw"].apply(lambda v: 3.0 if v == float("inf") else (v/mean_si if mean_si > 0 else 1.0))
    def _level(row):
        n_val = _safe_val(row, num_col)
        d_val = _safe_val(row, den_col)
        if n_val == 0: return "데이터없음"
        if d_val == 0: return "여유"
        si = row["SI_normalized"]
        for name, (lo, hi) in SATURATION_LEVELS.items():
            if lo <= si < hi: return name
        return "포화"
    df["saturation_level"] = df.apply(_level, axis=1)
    return df

def map_hospitals_to_dong(hospital_df: pd.DataFrame, geojson_path: str | Path = _DEFAULT_GEOJSON) -> pd.DataFrame:
    gdf_dong = gpd.read_file(geojson_path)
    hospital_df = hospital_df.copy()
    for col in ["XPos", "YPos"]:
        if col not in hospital_df.columns: hospital_df[col] = 0.0
    df_valid = hospital_df[(hospital_df["XPos"] != 0) & (hospital_df["YPos"] != 0)].copy()
    df_invalid = hospital_df[(hospital_df["XPos"] == 0) | (hospital_df["YPos"] == 0)].copy()
    if df_valid.empty:
        hospital_df["adm_cd2"] = None
        return hospital_df
    gdf_hosp = gpd.GeoDataFrame(df_valid, geometry=[Point(row.XPos, row.YPos) for _, row in df_valid.iterrows()], crs="EPSG:4326")
    joined = gpd.sjoin(gdf_hosp, gdf_dong[["adm_cd2", "adm_nm", "geometry"]], how="left", predicate="within")
    joined = joined[~joined.index.duplicated(keep="first")]
    result_valid = pd.DataFrame(joined.drop(columns=["geometry", "index_right", "adm_nm"], errors="ignore"))
    result_valid = result_valid.loc[:, ~result_valid.columns.duplicated(keep="first")]
    df_invalid["adm_cd2"] = None
    combined = pd.concat([result_valid, df_invalid], ignore_index=True)
    combined = combined.loc[:, ~combined.columns.duplicated(keep="first")]
    return combined

class DataMerger:
    def __init__(self, geojson_path: str | Path = _DEFAULT_GEOJSON):
        self.geojson_path = Path(geojson_path)

    def run(self, sgg_cd_pop: str, hira_sido_cd: str, sgg_name: str = "", specialty_codes: list[str] | None = None,
            year_month: str = "202412", cl_codes: list[str] | None = None,
            num_col: str = "총인구수", den_col: str = "clinic_count",
            analysis_level: str = "dong") -> dict:
        from modules.population_api import PopulationAPIClient, SIDO_CODES
        from modules.hospital_api import HospitalAPIClient, HIRA_SIDO_CODES, HIRA_SGG_MAP, HOSPITAL_COLUMNS

        pop_client = PopulationAPIClient()
        hosp_client = HospitalAPIClient()
        hira_to_pop = _get_hira_to_pop_map()

        # sido _finalize_key에서 사용할 구→시 코드 집합 (non-sido 경우 빈 집합)
        existing_sgg_codes: set = set()
        city_name_lookup: dict = {}

        # 1. 인구 데이터 수집
        if analysis_level in ["national", "sido"]:
            targets = SIDO_CODES.items() if analysis_level == "national" else [("", sgg_cd_pop)]
            if analysis_level == "sido":
                sgg_list = pop_client.get_sgg_list(sgg_cd_pop)
                targets = [(row["sggNm"], row["admmCd"]) for _, row in sgg_list.iterrows()]
                # sido 전용: 구→시 통합 룩업 빌드
                existing_sgg_codes = set(str(r["admmCd"])[:5] for _, r in sgg_list.iterrows())
                for _, r in sgg_list.iterrows():
                    c10 = str(r["admmCd"])
                    c5 = c10[:4] + "0"
                    if c10[:5] == c5:  # 5번째 자리 == "0" → city-level entry
                        city_name_lookup[c5] = r["sggNm"]
            frames = []
            for idx, (name, code) in enumerate(targets):
                try:
                    # national: sido 코드 → lv="2"(시군구 레벨) 사용
                    # sido:     sgg 코드  → lv="3"(행정동 레벨) 사용
                    _lv = "2" if analysis_level == "national" else "3"
                    df = pop_client.get_merged(code, year_month, lv=_lv)
                except Exception:
                    continue  # 개별 구/시도 실패 시 건너뛰고 계속 진행
                if not df.empty:
                    for c in df.columns:
                        if c not in ["admmCd", "행정동명", "통계년월", "시도명", "시군구명", "match_key"]:
                            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
                    num_df = df.select_dtypes(include=['number'])
                    if num_df.empty: continue
                    summ = num_df.sum().to_frame().T
                    if analysis_level == "national":
                        summ["행정동명"] = name if name else df["시도명"].iloc[0]
                        summ["match_key"] = str(code)[:2]
                    else:  # sido
                        city_5 = str(code)[:4] + "0"
                        if city_5 in existing_sgg_codes:
                            summ["match_key"] = city_5
                            summ["행정동명"] = city_name_lookup.get(city_5, name)
                        else:
                            summ["match_key"] = str(code)[:5]
                            summ["행정동명"] = name if name else df["시도명"].iloc[0]
                    summ["admmCd"] = str(code)
                    frames.append(summ)
            pop_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            # sido 분석: 같은 match_key 행 집계 (구→시 통합, 부천시 코드 불일치 포함)
            if not pop_df.empty and analysis_level == "sido":
                num_cols = pop_df.select_dtypes(include='number').columns.tolist()
                name_first = pop_df.groupby("match_key")["행정동명"].first()
                pop_df = pop_df.groupby("match_key", as_index=False)[num_cols].sum()
                pop_df["행정동명"] = pop_df["match_key"].map(name_first)
        else:
            pop_df = pop_client.get_merged(sgg_cd_pop, year_month, lv="3")
            if not pop_df.empty:
                pop_df["match_key"] = pop_df["admmCd"].astype(str)

        # 2. 병원 데이터 수집 (DB 직접 조회)
        # DB는 sido_cd 단위로 조회 후 spatial join(GPS)으로 행정동 배정
        # → HIRA_SGG_MAP 기반 sgg 코드 루프 불필요 (Excel 코드와 불일치 문제 해결)
        h_frames = []
        if analysis_level == "national":
            for sido_cd in HIRA_SIDO_CODES.values():
                try:
                    df = hosp_client.get_hospitals_multi(sido_cd, sgg_cd=None, specialty_codes=specialty_codes, cl_codes=cl_codes)
                    if not df.empty: h_frames.append(df)
                except: pass
        else:
            # sido / dong 공통: sido_cd 한 번에 조회, sgg 구분은 spatial join이 처리
            try:
                df = hosp_client.get_hospitals_multi(hira_sido_cd, sgg_cd=None, specialty_codes=specialty_codes, cl_codes=cl_codes)
                if not df.empty: h_frames.append(df)
            except: pass
        
        # h_frames가 비어있으면 컬럼 스키마를 보존한 빈 DataFrame 사용 (pd.DataFrame()은 컬럼 없음)
        hosp_all = pd.concat(h_frames, ignore_index=True) if h_frames else pd.DataFrame(columns=HOSPITAL_COLUMNS)

        # 3. 매핑 및 집계
        hosp_mapped = map_hospitals_to_dong(hosp_all, self.geojson_path)
        
        def _finalize_key(row):
            adm = str(_safe_val(row, "adm_cd2")) if pd.notna(_safe_val(row, "adm_cd2")) else ""
            if adm:
                if adm.startswith("41000"): pref = "36"
                elif adm.startswith("42"): pref = "51"
                elif adm.startswith("45"): pref = "52"
                else: pref = adm[:2]
                if analysis_level == "national": return pref
                if analysis_level == "sido":
                    city_5 = pref + adm[2:4] + "0"
                    return city_5 if city_5 in existing_sgg_codes else pref + adm[2:5]
                return pref + adm[2:10]
            
            p_sgg = hira_to_pop.get(str(_safe_val(row, "sgguCd")))
            if p_sgg:
                if analysis_level == "national": return str(p_sgg[:2])
                if analysis_level == "sido": return str(p_sgg)
            return None

        # 중복 컬럼 방어 처리 (geopandas sjoin 후 발생 가능)
        if hosp_mapped.columns.duplicated().any():
            hosp_mapped = hosp_mapped.loc[:, ~hosp_mapped.columns.duplicated(keep="first")]
        _key_result = hosp_mapped.apply(_finalize_key, axis=1)
        if isinstance(_key_result, pd.DataFrame):
            _key_result = _key_result.iloc[:, 0]
        hosp_mapped["match_key"] = _key_result
        hosp_summary = hosp_mapped.dropna(subset=["match_key"]).groupby(["match_key", "specialty_cd", "specialty_nm"], as_index=False).agg(
            clinic_count=("ykiho", "count"), specialist_count=("mdeptSdrCnt", "sum")
        )

        # 4. 결과 산출
        results = {cd: calc_saturation_index(merge_with_population(pop_df, hosp_summary, cd), num_col, den_col) for cd in specialty_codes}
        return {"population": pop_df, "hospitals": hosp_mapped, "hospital_summary": hosp_summary,
                "saturation": results, "analysis_level": analysis_level,
                "sgg_codes": existing_sgg_codes}
