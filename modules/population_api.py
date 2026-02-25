"""
행정안전부 행정동별 주민등록 인구 데이터 수집 모듈
"""

import re
import time
import requests
import pandas as pd

_RDOA_BASE = "https://rdoa.jumin.go.kr/openStats"
_JUMIN_BASE = "https://jumin.mois.go.kr"

_DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
}

SIDO_CODES = {
    "서울특별시": "1100000000", "부산광역시": "2600000000", "대구광역시": "2700000000",
    "인천광역시": "2800000000", "광주광역시": "2900000000", "대전광역시": "3000000000",
    "울산광역시": "3100000000", "세종특별자치시": "3600000000", "경기도": "4100000000",
    "강원특별자치도": "5100000000", "충청북도": "4300000000", "충청남도": "4400000000",
    "전북특별자치도": "5200000000", "전라남도": "4600000000", "경상북도": "4700000000",
    "경상남도": "4800000000", "제주특별자치도": "5000000000",
}

def _to_int(val: str) -> int:
    if not val: return 0
    clean = re.sub(r"[^0-9]", "", val)
    return int(clean) if clean.isdigit() else 0

def _parse_table_rows(html: str, min_cols: int = 2) -> list[list[str]]:
    rows_raw = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL | re.IGNORECASE)
    result = []
    for row in rows_raw:
        cells = [re.sub(r"<[^>]+>", "", c).strip() for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.DOTALL | re.IGNORECASE)]
        if len(cells) >= min_cols and cells[0]:
            result.append(cells)
    return result

class PopulationAPIClient:
    def __init__(self, request_delay: float = 0.3):
        self._delay = request_delay
        self._rdoa_session = requests.Session()
        self._rdoa_session.headers.update(_DEFAULT_HEADERS)
        self._jumin_session = requests.Session()
        self._jumin_session.headers.update(_DEFAULT_HEADERS)
        self._rdoa_jsid: str = ""
        self._jumin_initialized: bool = False

    def _init_rdoa_session(self) -> None:
        r = self._rdoa_session.get(f"{_RDOA_BASE}/selectConAdmmPpltnHh", timeout=10)
        match = re.search(r"jsessionid=([A-Za-z0-9+._-]+)", r.text)
        self._rdoa_jsid = match.group(1) if match else ""

    def _init_jumin_session(self) -> None:
        if not self._jumin_initialized:
            self._jumin_session.get(f"{_JUMIN_BASE}/ageStatMonth.do", timeout=10)
            self._jumin_initialized = True

    def _reset_jumin_session(self) -> None:
        """연결 끊김(RemoteDisconnected) 발생 시 세션을 새로 생성"""
        self._jumin_session = requests.Session()
        self._jumin_session.headers.update(_DEFAULT_HEADERS)
        self._jumin_initialized = False

    def get_sgg_list(self, sido_cd: str) -> pd.DataFrame:
        if not self._rdoa_jsid: self._init_rdoa_session()
        r = self._rdoa_session.get(f"{_RDOA_BASE}/selectSggList;jsessionid={self._rdoa_jsid}", params={"admmCd": sido_cd[:2]}, timeout=10)
        data = r.json() if r.text.strip().startswith("[") else []
        return pd.DataFrame(data)[["admmCd", "ctpvNm", "sggNm"]] if data else pd.DataFrame()

    def get_population(self, sgg_cd: str, year_month: str = "202412", lv: str = "3") -> pd.DataFrame:
        if not self._rdoa_jsid: self._init_rdoa_session()
        year, month = year_month[:4], year_month[4:6].zfill(2)
        target_cd = "0000000000" if lv == "1" else (sgg_cd[:2] + "00000000" if lv == "2" else sgg_cd)

        all_rows = []
        page = 1
        while True:
            payload = {
                "ctpvCd": sgg_cd[:2] if lv != "1" else "", "sggCd": sgg_cd if lv == "3" else "",
                "dongCd": "", "lv": lv, "regSeCd": "1",
                "srchFrYear": year, "srchFrMon": month, "srchToYear": year, "srchToMon": month,
                "curPage": str(page), "paramUrl": f"admmCd={target_cd}&lv={lv}&regSeCd=1&srchFrYm={year_month}&srchToYm={year_month}"
            }
            r = self._rdoa_session.post(f"{_RDOA_BASE}/selectConAdmmPpltnHh;jsessionid={self._rdoa_jsid}", data=payload, timeout=20)
            rows = _parse_table_rows(r.text)
            if not rows: break
            all_rows.extend(rows)
            if len(rows) < 10: break
            page += 1
            time.sleep(self._delay)

        data = []
        for row in all_rows:
            if len(row) < 8: continue
            row_str = "".join(row)
            if "전국" in row[0] or "소계" in row_str or "합계" in row_str: continue
            entry = {
                "통계년월": row[0].replace(".", ""), "admmCd": str(row[1]), "시도명": row[2],
                "시군구명": row[3] if len(row) > 3 else "",
                "행정동명": row[4] if lv == "3" else (row[3] if lv == "2" else row[2]),
                "총인구수": _to_int(row[-6]), "세대수": _to_int(row[-5]), "남자인구수": _to_int(row[-3]), "여자인구수": _to_int(row[-2]),
            }
            data.append(entry)
        
        df = pd.DataFrame(data)
        if not df.empty:
            for col in ["총인구수", "세대수", "남자인구수", "여자인구수"]:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
        return df.drop_duplicates(subset=["admmCd"]).reset_index(drop=True)

    def get_age_population(self, sgg_cd: str, year_month: str = "202412", lv: str = "3") -> pd.DataFrame:
        import calendar
        year, month = year_month[:4], year_month[4:6].zfill(2)
        last_day = calendar.monthrange(int(year), int(month))[1]
        org_type = {"1": "1", "2": "2", "3": "3"}.get(lv, "3")
        lvl1 = "0000000000" if lv == "1" else sgg_cd[:2] + "00000000"
        lvl2 = "0000000000" if lv == "1" else (sgg_cd[:2] + "00000000" if lv == "2" else sgg_cd)

        payload = {
            "tableChart": "T", "sltOrgType": org_type, "nowYear": "2025",
            "sltOrgLvl1": lvl1, "sltOrgLvl2": lvl2, "gender": "gender", "sum": "sum",
            "searchYearStart": year, "searchMonthStart": month, "searchYearEnd": year, "searchMonthEnd": month,
            "sltOrderType": "1", "sltOrderValue": "ASC", "sltArgTypes": "10", "category": "month",
            "startOrtnDe": f"{year}{month}01", "endOrtnDe": f"{year}{month}{last_day:02d}", "searchYearMonth": "month",
        }

        # 서버 연결 끊김 대비: 최대 3회 재시도 + 세션 재생성
        _MAX_RETRY = 3
        for attempt in range(_MAX_RETRY):
            try:
                self._init_jumin_session()
                r = self._jumin_session.post(f"{_JUMIN_BASE}/ageStatMonth.do", data=payload, timeout=30)
                break  # 성공 시 루프 탈출
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                if attempt < _MAX_RETRY - 1:
                    self._reset_jumin_session()
                    time.sleep(2 ** attempt)  # 1s → 2s → 4s 지수 백오프
                else:
                    return pd.DataFrame()  # 3회 모두 실패 시 빈 DataFrame
        rows = _parse_table_rows(r.text)
        if not rows: return pd.DataFrame()
        df_rows = []
        for row in rows:
            if len(row) < 13: continue
            admm_cd = str(row[0])
            if not admm_cd.isdigit() or admm_cd == "0000000000": continue
            age_vals = row[2:]
            entry = {"admmCd": admm_cd, "행정동명": row[1]}
            age_names = ["총인구수", "_v", "0_9세", "10_19세", "20_29세", "30_39세", "40_49세", "50_59세", "60_69세", "70_79세", "80_89세", "90_99세", "100세이상"]
            for i, nm in enumerate(age_names):
                if i < len(age_vals) and not nm.startswith("_"): entry[nm] = _to_int(age_vals[i])
            entry["20세이하인구"] = entry.get("0_9세", 0) + entry.get("10_19세", 0)
            entry["20_40세인구"] = entry.get("20_29세", 0) + entry.get("30_39세", 0)
            entry["40_60세인구"] = entry.get("40_49세", 0) + entry.get("50_59세", 0)
            entry["60세이상인구"] = entry.get("60_69세", 0) + entry.get("70_79세", 0) + entry.get("80_89세", 0) + entry.get("90_99세", 0) + entry.get("100세이상", 0)
            df_rows.append(entry)
        res_df = pd.DataFrame(df_rows)
        if not res_df.empty:
            for c in res_df.columns:
                if c not in ["admmCd", "행정동명"]:
                    res_df[c] = pd.to_numeric(res_df[c], errors='coerce').fillna(0).astype('int64')
        return res_df.reset_index(drop=True)

    def get_merged(self, sgg_cd: str, year_month: str = "202412", lv: str = "3") -> pd.DataFrame:
        pop_df = self.get_population(sgg_cd, year_month, lv=lv)
        time.sleep(self._delay)
        age_df = self.get_age_population(sgg_cd, year_month, lv=lv)
        if pop_df.empty: return age_df
        if age_df.empty: return pop_df
        return pop_df.merge(age_df.drop(columns=["행정동명", "총인구수"], errors="ignore"), on="admmCd", how="left")
