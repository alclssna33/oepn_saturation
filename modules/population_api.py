"""
행정안전부 행정동별 주민등록 인구 데이터 수집 모듈 (로컬 DB버전)
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

SIDO_CODES = {
    "서울특별시": "1100000000", "부산광역시": "2600000000", "대구광역시": "2700000000",
    "인천광역시": "2800000000", "광주광역시": "2900000000", "대전광역시": "3000000000",
    "울산광역시": "3100000000", "세종특별자치시": "3600000000", "경기도": "4100000000",
    "강원특별자치도": "5100000000", "충청북도": "4300000000", "충청남도": "4400000000",
    "전북특별자치도": "5200000000", "전라남도": "4600000000", "경상북도": "4700000000",
    "경상남도": "4800000000", "제주특별자치도": "5000000000",
}

class PopulationAPIClient:
    def __init__(self, request_delay: float = 0.0):
        # DB 기반이므로 delay는 무시하지만 호환성을 위해 유지
        self._delay = request_delay

    def get_sgg_list(self, sido_cd: str) -> pd.DataFrame:
        """
        특정 시도의 시군구 목록을 반환합니다.
        기존 반환 컬럼: admmCd, ctpvNm, sggNm
        """
        prefix = sido_cd[:2]
        query = f"SELECT adm_cd, adm_nm FROM population_house WHERE adm_cd LIKE '{prefix}%00000' AND adm_cd != '{prefix}00000000'"
        
        with _get_conn() as conn:
            df = pd.read_sql_query(query, conn)
            
        if df.empty:
            return pd.DataFrame()
            
        # ctpvNm (시도명), sggNm (시군구명) 분리
        df['ctpvNm'] = df['adm_nm'].apply(lambda x: x.split()[0] if isinstance(x, str) else "")
        df['sggNm'] = df['adm_nm'].apply(lambda x: x.split()[1] if isinstance(x, str) and len(x.split()) > 1 else "")
        df.rename(columns={'adm_cd': 'admmCd'}, inplace=True)
        return df[['admmCd', 'ctpvNm', 'sggNm']]

    def get_population(self, sgg_cd: str, year_month: str = "202412", lv: str = "3") -> pd.DataFrame:
        """
        세대수 및 기본 인구 데이터를 반환합니다.
        lv 1: 전국 시도, lv 2: 특정 시도 내 시군구, lv 3: 특정 시군구 내 행정동
        """
        with _get_conn() as conn:
            if lv == "1":
                query = "SELECT * FROM population_house WHERE adm_cd LIKE '%00000000'"
            elif lv == "2":
                prefix = sgg_cd[:2]
                query = f"SELECT * FROM population_house WHERE adm_cd LIKE '{prefix}%00000' AND adm_cd != '{prefix}00000000'"
            else: # lv == "3"
                prefix = sgg_cd[:5]
                query = f"SELECT * FROM population_house WHERE adm_cd LIKE '{prefix}%' AND adm_cd != '{prefix}00000'"
                
            df = pd.read_sql_query(query, conn)
            
        if df.empty:
            return pd.DataFrame()

        # 호환성 위해 이름 변경
        df.rename(columns={
            'adm_cd': 'admmCd',
            'total_pop': '총인구수',
            'households': '세대수',
            'male_pop': '남자인구수',
            'female_pop': '여자인구수'
        }, inplace=True)
        
        # 이름 분리
        df['시도명'] = df['adm_nm'].apply(lambda x: x.split()[0] if isinstance(x, str) else "")
        df['시군구명'] = df['adm_nm'].apply(lambda x: x.split()[1] if isinstance(x, str) and len(x.split()) > 1 else "")
        df['행정동명'] = df['adm_nm'].apply(lambda x: x.split()[2] if isinstance(x, str) and len(x.split()) > 2 else "")
        
        # 기초 체계 정리 (기존 API 형식 호환)
        df['통계년월'] = year_month
        return df

    def get_age_population(self, sgg_cd: str, year_month: str = "202412", lv: str = "3") -> pd.DataFrame:
        """
        연령별 인구 데이터를 반환합니다.
        """
        with _get_conn() as conn:
            if lv == "1":
                query = "SELECT * FROM population_age WHERE adm_cd LIKE '%00000000'"
            elif lv == "2":
                prefix = sgg_cd[:2]
                query = f"SELECT * FROM population_age WHERE adm_cd LIKE '{prefix}%00000' AND adm_cd != '{prefix}00000000'"
            else: # lv == "3"
                prefix = sgg_cd[:5]
                query = f"SELECT * FROM population_age WHERE adm_cd LIKE '{prefix}%' AND adm_cd != '{prefix}00000'"
                
            df = pd.read_sql_query(query, conn)

        if df.empty:
            return pd.DataFrame()

        df.rename(columns={
            'adm_cd': 'admmCd',
            'adm_nm': '행정동명',
            'total_pop': '총인구수',
            'age_0_9': '0_9세',
            'age_10_19': '10_19세',
            'age_20_29': '20_29세',
            'age_30_39': '30_39세',
            'age_40_49': '40_49세',
            'age_50_59': '50_59세',
            'age_60_69': '60_69세',
            'age_70_79': '70_79세',
            'age_80_89': '80_89세',
            'age_90_99': '90_99세',
            'age_100_plus': '100세이상'
        }, inplace=True)
        
        # 합산 컬럼 (호환성 목적)
        df["20세이하인구"] = df.get("0_9세", 0) + df.get("10_19세", 0)
        df["20_40세인구"] = df.get("20_29세", 0) + df.get("30_39세", 0)
        df["40_60세인구"] = df.get("40_49세", 0) + df.get("50_59세", 0)
        df["60세이상인구"] = df.get("60_69세", 0) + df.get("70_79세", 0) + df.get("80_89세", 0) + df.get("90_99세", 0) + df.get("100세이상", 0)

        # 시스템 상 행정동명은 마지막 단어만 (읍면동) 리턴했었음
        if lv == "3":
            df['행정동명'] = df['행정동명'].apply(lambda x: x.split()[-1] if isinstance(x, str) else "")

        return df

    def get_merged(self, sgg_cd: str, year_month: str = "202412", lv: str = "3") -> pd.DataFrame:
        pop_df = self.get_population(sgg_cd, year_month, lv=lv)
        age_df = self.get_age_population(sgg_cd, year_month, lv=lv)
        
        if pop_df.empty: return age_df
        if age_df.empty: return pop_df
        
        # join 시 중복 컬럼 제거 (행정동명, 총인구수)
        drop_cols = ["행정동명", "총인구수"]
        age_df_clean = age_df.drop(columns=[c for c in drop_cols if c in age_df.columns], errors="ignore")
        
        return pop_df.merge(age_df_clean, on="admmCd", how="left")
