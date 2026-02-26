"""
건강보험심사평가원 병의원 데이터 수집 모듈 (로컬 DB버전)
"""

import sqlite3
import pandas as pd
import os

# DB 경로 설정
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'saturation.db')

def _get_conn():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"로컬 DB를 찾을 수 없습니다: {DB_PATH}")
    return sqlite3.connect(DB_PATH)


# HIRA 시도 코드 (심평원 실 데이터 기반)
HIRA_SIDO_CODES = {
    "서울특별시": "110000", "부산광역시": "210000", "인천광역시": "220000", "대구광역시": "230000",
    "광주광역시": "240000", "대전광역시": "250000", "울산광역시": "260000", "세종특별자치시": "410000",
    "경기도": "310000", "강원특별자치도": "320000", "충청북도": "330000", "충청남도": "340000",
    "전북특별자치도": "350000", "전라남도": "360000", "경상북도": "370000", "경상남도": "380000", "제주특별자치도": "390000",
}

# 심평원 코드표 (SQLite 변환 시에도 동일 사용)
SPECIALTY_CODES = {
    "01": "내과",           "02": "신경과",           "03": "정신건강의학과",
    "04": "외과",           "05": "정형외과",          "06": "신경외과",
    "07": "흉부외과",       "08": "성형외과",          "09": "마취통증의학과",
    "10": "산부인과",       "11": "소아청소년과",       "12": "안과",
    "13": "이비인후과",     "14": "피부과",            "15": "비뇨의학과",
    "16": "영상의학과",     "17": "방사선종양학과",     "18": "병리과",
    "19": "진단검사의학과", "20": "결핵과",            "21": "재활의학과",
    "22": "핵의학과",       "23": "가정의학과",         "24": "응급의학과",
    "25": "직업환경의학과", "26": "예방의학과",
    "49": "치과",           "52": "치과교정과",        "53": "소아치과",
    "54": "치주과",         "55": "치과보존과",        "61": "통합치의학과",
    "80": "한방내과",       "81": "한방부인과",        "82": "한방소아과",
    "83": "한방안이비인후피부과", "84": "한방신경정신과",
    "85": "침구과",         "86": "한방재활의학과",    "87": "사상체질과",
}

HOSPITAL_COLUMNS = [
    "yadmNm", "addr", "emdongNm", "sidoCd", "sidoCdNm", "sgguCd", "sgguCdNm",
    "clCd", "clCdNm", "specialty_cd", "specialty_nm", "mdeptSdrCnt", "drTotCnt", "XPos", "YPos", "ykiho",
    "estbDd",
]

# 인구API 시군구 코드(앞5자리) → HIRA 시군구 코드(6자리) 매핑 테이블 유지는 필요하지만, DB에서 시군구 코드를 뽑아낼 수도 있습니다. (하위 호환성을 위해 남겨둠)
HIRA_SGG_MAP = {
    "11110": "110016", "11140": "110017", "11170": "110014", "11200": "110011",
    "11215": "110023", "11230": "110007", "11260": "110019", "11290": "110012",
    "11305": "110024", "11320": "110006", "11350": "110022", "11380": "110015",
    "11410": "110010", "11440": "110009", "11470": "110020", "11500": "110003",
    "11530": "110005", "11545": "110025", "11560": "110013", "11590": "110008",
    "11620": "110004", "11650": "110021", "11680": "110001", "11710": "110018", "11740": "110002",
    "26110": "210012", "26140": "210008", "26170": "210002", "26200": "210011",
    "26230": "210004", "26260": "210003", "26290": "210001", "26320": "210005",
    "26350": "210013", "26380": "210007", "26410": "210015", "26440": "210014",
    "26470": "210010", "26500": "210009", "26530": "210006", "26710": "210100",
    "27110": "230006", "27140": "230002", "27170": "230003", "27200": "230004",
    "27230": "230001", "27260": "230005", "27290": "230007", "27710": "230100", "27720": "230100",
    "28110": "220004", "28140": "220002", "28177": "220001", "28185": "220007",
    "28200": "220006", "28237": "220003", "28245": "220008", "28260": "220005",
    "28710": "220100", "28720": "220200",
    "29110": "240001", "29140": "240002", "29155": "240003", "29170": "240004", "29200": "240005",
    "30110": "250001", "30140": "250002", "30170": "250003", "30200": "250004", "30230": "250005",
    "31110": "260001", "31140": "260002", "31170": "260003", "31200": "260004", "31710": "260005",
    "36110": "410000",
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
    "42110": "320001", "42130": "320002", "42150": "320003", "42170": "320004",
    "42190": "320005", "42210": "320006", "42230": "320007", "42720": "320008",
    "42730": "320009", "42740": "320010", "42750": "320011", "42760": "320012",
    "42770": "320013", "42780": "320014", "42790": "320015", "42800": "320016",
    "42810": "320017", "42820": "320018",
    "43110": "330001", "43130": "330005", "43150": "330006", "43720": "330007",
    "43730": "330008", "43740": "330009", "43745": "330010", "43750": "330011",
    "43760": "330012", "43770": "330013", "43800": "330140",
    "44130": "340001", "44150": "340003", "44180": "340004", "44200": "340005",
    "44210": "340006", "44230": "340007", "44250": "340008", "44270": "340009",
    "44710": "340010", "44760": "340011", "44770": "340012", "44790": "340013",
    "44800": "340014", "44810": "340015", "44825": "340016",
    "45110": "350001", "45130": "350003", "45140": "350004", "45180": "350005",
    "45190": "350006", "45210": "350007", "45710": "350008", "45720": "350009",
    "45730": "350010", "45740": "350011", "45750": "350012", "45770": "350013",
    "45790": "350014", "45800": "350015",
    "46110": "360001", "46130": "360002", "46150": "360003", "46170": "360004",
    "46230": "360005", "46710": "360006", "46720": "360007", "46730": "360008",
    "46770": "360009", "46780": "360010", "46790": "360011", "46800": "360012",
    "46810": "360013", "46820": "360014", "46830": "360015", "46840": "360016",
    "46860": "360017", "46870": "360018", "46880": "360019", "46900": "360020",
    "46910": "360021", "46920": "360022",
    "47110": "370001", "47130": "370003", "47150": "370004", "47170": "370005",
    "47190": "370006", "47210": "370007", "47230": "370008", "47250": "370009",
    "47280": "370100", "47290": "370011", "47730": "370012", "47750": "370013",
    "47760": "370014", "47770": "370015", "47820": "370016", "47830": "370017",
    "47840": "370018", "47850": "370019", "47900": "370020", "47920": "370021",
    "47930": "370022", "47940": "370023",
    "48120": "380001", "48170": "380006", "48220": "380007", "48240": "380008",
    "48250": "380009", "48270": "380100", "48310": "380011", "48330": "380012",
    "48720": "380013", "48730": "380014", "48740": "380150", "48820": "380016",
    "48840": "380017", "48850": "380018", "48860": "380019", "48870": "380020",
    "48880": "380021", "48890": "380022",
    "50110": "390001", "50130": "390002",
}

class HospitalAPIClient:
    def __init__(self, api_key: str = "", request_delay: float = 0.0):
        pass # 호환성을 위해 남겨둠

    def get_hospitals(self, sido_cd: str, sgg_cd: str | None = None, specialty_cd: str = "01",
                       cl_codes: list[str] | None = None, include_hospitals: bool = True) -> pd.DataFrame:
        
        if cl_codes is None: 
            cl_codes = ["31", "21", "11"] if include_hospitals else ["31"]
        
        # SQLite JOIN 쿼리로 한 번에 병원 정보와 진료과목을 가져옵니다.
        cl_cd_clause = f"h.cl_cd IN ({','.join(['?' for _ in cl_codes])})"
        
        query = f"""
        SELECT 
            h.ykiho, h.hosp_nm as yadmNm, h.addr, h.emdong_nm as emdongNm, 
            h.sido_cd as sidoCd, '{sido_cd}' as sidoCdNm, 
            h.sigungu_cd as sgguCd, '{sgg_cd}' as sgguCdNm,
            h.cl_cd as clCd, h.cl_cd_nm as clCdNm,
            s.dgsbjt_cd as specialty_cd, s.dgsbjt_cd_nm as specialty_nm,
            s.dr_cnt as mdeptSdrCnt, h.dr_tot_cnt as drTotCnt,
            h.x_pos as XPos, h.y_pos as YPos, h.estb_dd as estbDd
        FROM hospital_info h
        INNER JOIN hospital_specialty s ON h.ykiho = s.ykiho
        WHERE h.sido_cd = ?
          AND {cl_cd_clause}
          AND s.dgsbjt_cd = ?
        """
        
        params = [sido_cd] + cl_codes + [specialty_cd]
        
        if sgg_cd:
            query += " AND h.sigungu_cd = ?"
            params.append(sgg_cd)
            
        with _get_conn() as conn:
            df = pd.read_sql_query(query, conn, params=params)
            
        if df.empty:
            return pd.DataFrame(columns=HOSPITAL_COLUMNS)

        # 데이터 타입 정리 (기존 API 형식과 유사하게)
        for col in ["mdeptSdrCnt", "drTotCnt"]:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
            
        for col in ["XPos", "YPos"]:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        # 누락된 컬럼 빈 문자열 처리
        for col in HOSPITAL_COLUMNS:
            if col not in df.columns: 
                df[col] = ""

        return df[HOSPITAL_COLUMNS].reset_index(drop=True)

    def get_hospitals_multi(self, sido_cd: str, sgg_cd: str | None = None,
                           specialty_codes: list[str] | None = None, cl_codes: list[str] | None = None) -> pd.DataFrame:
        if specialty_codes is None: 
            specialty_codes = ["01", "05", "11", "12", "14"]
            
        # SQLite에서는 in 쿼리로 한방에 던질 수 있어 속도가 훨씬 빠릅니다.
        if cl_codes is None: 
            cl_codes = ["31", "21", "11"]
            
        cl_cd_clause = f"h.cl_cd IN ({','.join(['?' for _ in cl_codes])})"
        spec_clause = f"s.dgsbjt_cd IN ({','.join(['?' for _ in specialty_codes])})"
        
        query = f"""
        SELECT 
            h.ykiho, h.hosp_nm as yadmNm, h.addr, h.emdong_nm as emdongNm, 
            h.sido_cd as sidoCd, '{sido_cd}' as sidoCdNm, 
            h.sigungu_cd as sgguCd, '{sgg_cd}' as sgguCdNm,
            h.cl_cd as clCd, h.cl_cd_nm as clCdNm,
            s.dgsbjt_cd as specialty_cd, s.dgsbjt_cd_nm as specialty_nm,
            s.dr_cnt as mdeptSdrCnt, h.dr_tot_cnt as drTotCnt,
            h.x_pos as XPos, h.y_pos as YPos, h.estb_dd as estbDd
        FROM hospital_info h
        INNER JOIN hospital_specialty s ON h.ykiho = s.ykiho
        WHERE h.sido_cd = ?
          AND {cl_cd_clause}
          AND {spec_clause}
        """
        
        params = [sido_cd] + cl_codes + specialty_codes
        
        if sgg_cd:
            query += " AND h.sigungu_cd = ?"
            params.append(sgg_cd)
            
        with _get_conn() as conn:
            df = pd.read_sql_query(query, conn, params=params)
            
        if df.empty:
            return pd.DataFrame(columns=HOSPITAL_COLUMNS)

        for col in ["mdeptSdrCnt", "drTotCnt"]:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
        for col in ["XPos", "YPos"]:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            
        return df[HOSPITAL_COLUMNS].reset_index(drop=True)

    def get_sgg_list(self, sido_cd: str) -> dict[str, str]:
        # sido_cd (심평원 시도코드) 에 속하는 시군구를 반환
        query = "SELECT DISTINCT sigungu_cd, addr FROM hospital_info WHERE sido_cd = ?"
        with _get_conn() as conn:
            df = pd.read_sql_query(query, conn, params=[sido_cd])
            
        if df.empty:
            return {}
            
        res = {}
        for _, row in df.iterrows():
            cd = str(row['sigungu_cd'])
            addr = str(row['addr'])
            # 주소에서 "서울특별시 종로구 어쩌고" -> "종로구" 추출
            parts = addr.split()
            if len(parts) > 1:
                sgg_nm = parts[1]
                res[sgg_nm] = cd
                
        return res
