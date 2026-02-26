"""
공공데이터 API를 주기적으로 호출하여 로컬 DB 및 Supabase를 업데이트하는 스크립트.
기존의 population_api.py 와 hospital_api.py 에서 사용하던 로직을 그대로 가져와 활용합니다.
"""

import sqlite3
import pandas as pd
import requests
import time
import os
import re
import urllib.parse
from datetime import datetime

# ======================================================================
# 1. 설정 및 초기화
# ======================================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'saturation.db')
ENV_PATH = os.path.join(BASE_DIR, '.env')

def load_env():
    env_vars = {}
    if os.path.exists(ENV_PATH):
        with open(ENV_PATH, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    if '=' in line:
                        key, val = line.split('=', 1)
                        env_vars[key.strip()] = val.strip()
    return env_vars

env = load_env()
PUBLIC_DATA_API_KEY = env.get('PUBLIC_DATA_API_KEY', '')

# HIRA 병의원 API
HIRA_BASE_URL = "http://apis.data.go.kr/B551182/hospInfoServicev2/getHospBasisList"
# 행안부 인구 API
RDOA_BASE = "https://rdoa.jumin.go.kr/openStats"

# ======================================================================
# 2. 업데이트 로직
# ======================================================================
def update_hospital_data():
    print("\n[업데이트] 1. 병의원 데이터(전국 의원) 최신화 시작...")
    import xml.etree.ElementTree as ET
    
    # HIRA 시도 코드 목록 (일부 발췌, 실제로는 앞서 정의한 17개 시도 모두 순회해야함)
    sido_list = ["110000", "210000", "310000"] # 시연용 3곳 (서울, 부산, 경기)
    cl_code = "31" # 의원급만 우선 업데이트
    
    conn = sqlite3.connect(DB_PATH)
    session = requests.Session()
    
    all_hospitals = []
    
    for sido_cd in sido_list:
        print(f" - 시도코드 {sido_cd} 수집 중...")
        page = 1
        while True:
            params = {
                "serviceKey": urllib.parse.unquote(PUBLIC_DATA_API_KEY), 
                "pageNo": page, 
                "numOfRows": 1000, 
                "sidoCd": sido_cd,
                "clCd": cl_code
            }
            try:
                r = session.get(HIRA_BASE_URL, params=params, timeout=15)
                root = ET.fromstring(r.content)
                result_code = root.findtext(".//resultCode") or "00"
                if result_code != "00":
                    break
                
                items = root.findall(".//item")
                if not items:
                    break
                    
                for item in items:
                    row = {child.tag: (child.text or "").strip() for child in item}
                    all_hospitals.append(row)
                    
                total = int(root.findtext(".//totalCount") or "0")
                if len(all_hospitals) >= total or len(items) < 1000:
                    break
                    
                page += 1
                time.sleep(0.5)
            except Exception as e:
                print(f"   * API 오류 발생: {e}")
                break

    if all_hospitals:
        df_new = pd.DataFrame(all_hospitals)
        hosp_cols = {
            'ykiho': 'ykiho',
            'yadmNm': 'hosp_nm',
            'clCd': 'cl_cd',
            'clCdNm': 'cl_cd_nm',
            'sidoCd': 'sido_cd',
            'sgguCd': 'sigungu_cd',
            'emdongNm': 'emdong_nm',
            'addr': 'addr',
            'estbDd': 'estb_dd',
            'drTotCnt': 'dr_tot_cnt',
            'XPos': 'x_pos',
            'YPos': 'y_pos'
        }
        
        # 있는 컬럼만 남기기
        keep_cols = [c for c in hosp_cols.keys() if c in df_new.columns]
        df_new = df_new[keep_cols]
        df_new.rename(columns=hosp_cols, inplace=True)
        
        # SQLite Upsert 로직 (간소화: 기존 테이블 읽어서 ykiho 기준으로 병합 후 덮어쓰기)
        print(" - 수집 완료. 로컬 DB 병합 중...")
        df_old = pd.read_sql_query("SELECT * FROM hospital_info", conn)
        
        # ykiho를 인덱스로 잡고 update (새로운 값이 우선)
        df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=['ykiho'], keep='last')
        df_combined.to_sql('hospital_info', conn, if_exists='replace', index=False)
        print(f" - 병원 정보 DB 업데이트 완료: 총 {len(df_combined)} 건 보존됨.")
    
    conn.close()

def update_population_data():
    print("\n[업데이트] 2. 인구 데이터 최신화 (아직 미구현. API 세션/토큰 취득 등 복잡도가 높아 별도 스케줄링 필요)")
    print(" - 인구 데이터는 월 1회 갱신되므로 엑셀 다운로드 수동 적재 방식(Phase 1) 유지를 권장합니다.")


def main():
    print(f"=== 개원포화도 DB 업데이트 스크립트 시작 ({datetime.now()}) ===")
    if not PUBLIC_DATA_API_KEY:
        print("경고: .env 파일에 PUBLIC_DATA_API_KEY 가 없습니다. 심평원 API 호출이 실패할 수 있습니다.")
        
    update_hospital_data()
    update_population_data()
    
    print("\n=== 모든 업데이트 프로세스 종료 ===")
    print("향후 윈도우 작업 스케줄러나 cron을 사용하여 이 스크립트를 주기적으로 실행하세요.")
    print("Supabase 온라인 DB 연동 시 `scripts/migrate_to_supabase.py` 를 연쇄 실행하면 됩니다.")

if __name__ == "__main__":
    main()
