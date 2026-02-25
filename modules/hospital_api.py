"""
건강보험심사평가원 병의원 데이터 수집 모듈
"""

import time
import xml.etree.ElementTree as ET
import pandas as pd
import requests
from config import PUBLIC_DATA_API_KEY

_BASE_URL = "http://apis.data.go.kr/B551182/hospInfoServicev2/getHospBasisList"
_PAGE_SIZE = 100

# HIRA 시도 코드 (심평원 실 데이터 기반)
HIRA_SIDO_CODES = {
    "서울특별시": "110000", "부산광역시": "210000", "인천광역시": "220000", "대구광역시": "230000",
    "광주광역시": "240000", "대전광역시": "250000", "울산광역시": "260000", "세종특별자치시": "410000",
    "경기도": "310000", "강원특별자치도": "320000", "충청북도": "330000", "충청남도": "340000",
    "전북특별자치도": "350000", "전라남도": "360000", "경상북도": "370000", "경상남도": "380000", "제주특별자치도": "390000",
}

# 인구API 시군구 코드(앞5자리) → HIRA 시군구 코드(6자리) 매핑 테이블
HIRA_SGG_MAP: dict[str, str] = {
    # 서울
    "11110": "110016", "11140": "110017", "11170": "110014", "11200": "110011",
    "11215": "110023", "11230": "110007", "11260": "110019", "11290": "110012",
    "11305": "110024", "11320": "110006", "11350": "110022", "11380": "110015",
    "11410": "110010", "11440": "110009", "11470": "110020", "11500": "110003",
    "11530": "110005", "11545": "110025", "11560": "110013", "11590": "110008",
    "11620": "110004", "11650": "110021", "11680": "110001", "11710": "110018", "11740": "110002",
    # 부산
    "26110": "210012", "26140": "210008", "26170": "210002", "26200": "210011",
    "26230": "210004", "26260": "210003", "26290": "210001", "26320": "210005",
    "26350": "210013", "26380": "210007", "26410": "210015", "26440": "210014",
    "26470": "210010", "26500": "210009", "26530": "210006", "26710": "210100",
    # 대구
    "27110": "230006", "27140": "230002", "27170": "230003", "27200": "230004",
    "27230": "230001", "27260": "230005", "27290": "230007", "27710": "230100", "27720": "230100",
    # 인천
    "28110": "220004", "28140": "220002", "28177": "220001", "28185": "220007",
    "28200": "220006", "28237": "220003", "28245": "220008", "28260": "220005",
    "28710": "220100", "28720": "220200",
    # 광주
    "29110": "240001", "29140": "240002", "29155": "240003", "29170": "240004", "29200": "240005",
    # 대전
    "30110": "250001", "30140": "250002", "30170": "250003", "30200": "250004", "30230": "250005",
    # 울산
    "31110": "260001", "31140": "260002", "31170": "260003", "31200": "260004", "31710": "260005",
    # 세종
    "36110": "410000",
    # 경기
    "41110": "310001", "41113": "310002", "41115": "310003", "41117": "310004",
    "41130": "310005", "41131": "310006", "41133": "310007", "41150": "310008",
    "41170": "310009", "41171": "310100", "41190": "310011", "41210": "310012",
    "41220": "310013", "41250": "310014", "41270": "310015", "41271": "310160",
    "41280": "310017", "41285": "310180", "41287": "310190", "41290": "310020",
    "41310": "310021", "41360": "310022", "41370": "310023", "41390": "310024",
    "41410": "310025", "41430": "310026", "41450": "310027", "41460": "310028",
    "41461": "310290", "41463": "310300", "41480": "310031", "41500": "310032",
    "41550": "310033", "41570": "310034", "41590": "310035", "41610": "310036",
    "41630": "310037", "41650": "310038", "41670": "310039", "41800": "310400",
    "41820": "310410", "41830": "310420",
    # 강원
    "42110": "320001", "42130": "320002", "42150": "320003", "42170": "320004",
    "42190": "320005", "42210": "320006", "42230": "320007", "42720": "320008",
    "42730": "320009", "42740": "320010", "42750": "320011", "42760": "320012",
    "42770": "320013", "42780": "320014", "42790": "320015", "42800": "320016",
    "42810": "320017", "42820": "320018",
    # 충북
    "43110": "330001", "43130": "330005", "43150": "330006", "43720": "330007",
    "43730": "330008", "43740": "330009", "43745": "330010", "43750": "330011",
    "43760": "330012", "43770": "330013", "43800": "330140",
    # 충남
    "44130": "340001", "44150": "340003", "44180": "340004", "44200": "340005",
    "44210": "340006", "44230": "340007", "44250": "340008", "44270": "340009",
    "44710": "340010", "44760": "340011", "44770": "340012", "44790": "340013",
    "44800": "340014", "44810": "340015", "44825": "340016",
    # 전북
    "45110": "350001", "45130": "350003", "45140": "350004", "45180": "350005",
    "45190": "350006", "45210": "350007", "45710": "350008", "45720": "350009",
    "45730": "350010", "45740": "350011", "45750": "350012", "45770": "350013",
    "45790": "350014", "45800": "350015",
    # 전남
    "46110": "360001", "46130": "360002", "46150": "360003", "46170": "360004",
    "46230": "360005", "46710": "360006", "46720": "360007", "46730": "360008",
    "46770": "360009", "46780": "360010", "46790": "360011", "46800": "360012",
    "46810": "360013", "46820": "360014", "46830": "360015", "46840": "360016",
    "46860": "360017", "46870": "360018", "46880": "360019", "46900": "360020",
    "46910": "360021", "46920": "360022",
    # 경북
    "47110": "370001", "47130": "370003", "47150": "370004", "47170": "370005",
    "47190": "370006", "47210": "370007", "47230": "370008", "47250": "370009",
    "47280": "370100", "47290": "370011", "47730": "370012", "47750": "370013",
    "47760": "370014", "47770": "370015", "47820": "370016", "47830": "370017",
    "47840": "370018", "47850": "370019", "47900": "370020", "47920": "370021",
    "47930": "370022", "47940": "370023",
    # 경남
    "48120": "380001", "48170": "380006", "48220": "380007", "48240": "380008",
    "48250": "380009", "48270": "380100", "48310": "380011", "48330": "380012",
    "48720": "380013", "48730": "380014", "48740": "380150", "48820": "380016",
    "48840": "380017", "48850": "380018", "48860": "380019", "48870": "380020",
    "48880": "380021", "48890": "380022",
    # 제주
    "50110": "390001", "50130": "390002",
}

SPECIALTY_CODES = {
    # ── 의과 (건강보험심사평가원 dgsbjtCd 기준) ──────────────────────────────
    "01": "내과",           "02": "신경과",           "03": "정신건강의학과",
    "04": "외과",           "05": "정형외과",          "06": "신경외과",
    "07": "흉부외과",       "08": "성형외과",          "09": "마취통증의학과",
    "10": "산부인과",       "11": "소아청소년과",       "12": "안과",
    "13": "이비인후과",     "14": "피부과",            "15": "비뇨의학과",
    "16": "영상의학과",     "17": "방사선종양학과",     "18": "병리과",
    "19": "진단검사의학과", "20": "결핵과",            "21": "재활의학과",
    "22": "핵의학과",       "23": "가정의학과",         "24": "응급의학과",
    "25": "직업환경의학과", "26": "예방의학과",
    # ── 치과 ─────────────────────────────────────────────────────────────────
    "49": "치과",           "52": "치과교정과",        "53": "소아치과",
    "54": "치주과",         "55": "치과보존과",        "61": "통합치의학과",
    # ── 한방 ─────────────────────────────────────────────────────────────────
    "80": "한방내과",       "81": "한방부인과",        "82": "한방소아과",
    "83": "한방안이비인후피부과", "84": "한방신경정신과",
    "85": "침구과",         "86": "한방재활의학과",    "87": "사상체질과",
}

# 병원 DataFrame의 기본 컬럼 스키마 (빈 DataFrame 생성 및 외부 import용)
HOSPITAL_COLUMNS = [
    "yadmNm", "addr", "emdongNm", "sidoCd", "sidoCdNm", "sgguCd", "sgguCdNm",
    "clCd", "clCdNm", "specialty_cd", "specialty_nm", "mdeptSdrCnt", "drTotCnt", "XPos", "YPos", "ykiho",
    "estbDd",
]

def _to_int(val) -> int:
    try: return int(val) if val else 0
    except: return 0

def _to_float(val) -> float:
    try: return float(val) if val else 0.0
    except: return 0.0

def _parse_response(xml_bytes: bytes) -> tuple[int, list[dict]]:
    root = ET.fromstring(xml_bytes)
    # HIRA API 오류 응답 감지 (HTTP 200이어도 XML 내부에 에러코드가 올 수 있음)
    result_code = root.findtext(".//resultCode") or "00"
    if result_code != "00":
        result_msg = root.findtext(".//resultMsg") or "Unknown error"
        raise RuntimeError(f"HIRA API 오류 [{result_code}]: {result_msg}")
    total = _to_int(root.findtext(".//totalCount"))
    items = []
    for item in root.findall(".//item"):
        row = {child.tag: (child.text or "").strip() for child in item}
        items.append(row)
    return total, items

class HospitalAPIClient:
    def __init__(self, api_key: str = PUBLIC_DATA_API_KEY, request_delay: float = 0.3):
        self._key = api_key
        self._delay = request_delay
        self._session = requests.Session()

    def _fetch_page(self, sido_cd: str | None, sgg_cd: str | None, specialty_cd: str,
                    cl_codes: list[str], page: int) -> tuple[int, list[dict]]:
        params = {"serviceKey": self._key, "pageNo": page, "numOfRows": _PAGE_SIZE, "dgsbjtCd": specialty_cd}
        if sido_cd: params["sidoCd"] = sido_cd
        if sgg_cd: params["sgguCd"] = sgg_cd
        # HIRA API는 clCd 다중값(콤마구분)을 지원하지 않음 → 단일값일 때만 전달, 복수면 필터 생략(전체 종별 반환)
        if cl_codes and len(cl_codes) == 1:
            params["clCd"] = cl_codes[0]
        r = self._session.get(_BASE_URL, params=params, timeout=15)
        r.raise_for_status()
        return _parse_response(r.content)

    def get_hospitals(self, sido_cd: str, sgg_cd: str | None = None, specialty_cd: str = "01",
                       cl_codes: list[str] | None = None, include_hospitals: bool = True) -> pd.DataFrame:
        if cl_codes is None: cl_codes = ["31", "21", "11"] if include_hospitals else ["31"]
        all_items, page = [], 1
        total_first, items_first = self._fetch_page(sido_cd, sgg_cd, specialty_cd, cl_codes, page)
        all_items.extend(items_first)
        if items_first and len(all_items) < total_first:
            while True:
                page += 1
                time.sleep(self._delay)
                total, items = self._fetch_page(sido_cd, sgg_cd, specialty_cd, cl_codes, page)
                all_items.extend(items)
                if not items or len(all_items) >= total: break
        
        _KEEP = ["yadmNm", "addr", "emdongNm", "sidoCd", "sidoCdNm", "sgguCd", "sgguCdNm",
                 "clCd", "clCdNm", "specialty_cd", "specialty_nm", "mdeptSdrCnt", "drTotCnt", "XPos", "YPos", "ykiho",
                 "estbDd"]
        if not all_items: return pd.DataFrame(columns=_KEEP)
        df = pd.DataFrame(all_items)
        for c in df.columns:
            if c.lower() == "xpos": df.rename(columns={c: "XPos"}, inplace=True)
            elif c.lower() == "ypos": df.rename(columns={c: "YPos"}, inplace=True)
        for col in ["mdeptSdrCnt", "drTotCnt"]:
            if col in df.columns: df[col] = df[col].apply(_to_int)
        for col in ["XPos", "YPos"]:
            if col in df.columns: df[col] = df[col].apply(_to_float)
            else: df[col] = 0.0
        df["specialty_cd"] = specialty_cd
        df["specialty_nm"] = SPECIALTY_CODES.get(specialty_cd, specialty_cd)
        for col in _KEEP:
            if col not in df.columns: df[col] = ""
        return df[_KEEP].reset_index(drop=True)

    def get_hospitals_multi(self, sido_cd: str, sgg_cd: str | None = None,
                           specialty_codes: list[str] | None = None, cl_codes: list[str] | None = None) -> pd.DataFrame:
        if specialty_codes is None: specialty_codes = ["01", "05", "11", "12", "14"]
        frames = []
        for code in specialty_codes:
            df = self.get_hospitals(sido_cd, sgg_cd, code, cl_codes=cl_codes)
            frames.append(df)
            time.sleep(self._delay)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def get_sgg_list(self, sido_cd: str) -> dict[str, str]:
        params = {"serviceKey": self._key, "sidoCd": sido_cd, "numOfRows": 200}
        try:
            r = self._session.get(_BASE_URL, params=params, timeout=10)
            root = ET.fromstring(r.content)
            res = {}
            for item in root.findall(".//item"):
                cd, nm = item.findtext("sgguCd"), item.findtext("sgguCdNm")
                if cd and nm: res[nm.split()[-1]] = cd
            return res
        except: return {}
