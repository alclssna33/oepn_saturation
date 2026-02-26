import sqlite3
import pandas as pd
import requests
import json
import os
import time

# 환경 변수 로드 (python-dotenv가 설치 안되어 있을 수도 있으므로 자체 파싱 병행)
def load_env():
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    env_vars = {}
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, val = line.split('=', 1)
                    env_vars[key.strip()] = val.strip()
    return env_vars

env = load_env()
SUPABASE_URL = env.get('SUPABASE_URL')
SUPABASE_KEY = env.get('SUPABASE_ANON_KEY') or env.get('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    print("오류: .env 파일에 SUPABASE_URL 및 SUPABASE_KEY (또는 SUPABASE_ANON_KEY)를 설정해주세요.")
    print("예시:")
    print("SUPABASE_URL=https://xxxx.supabase.co")
    print("SUPABASE_KEY=eyJh...")
    exit(1)

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'saturation.db')

def bulk_insert_to_supabase(table_name, df, batch_size=500):
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    total_rows = len(df)
    print(f"[{table_name}] 총 {total_rows} 건 마이그레이션 시작...")
    
    # NaN 처리
    df = df.where(pd.notnull(df), None)
    
    records = df.to_dict('records')
    inserted = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        response = requests.post(url, headers=HEADERS, json=batch)
        
        if response.status_code in [201, 204]:
            inserted += len(batch)
            print(f" - {inserted}/{total_rows} 건 업로드 완료")
        else:
            print(f"오류 발생 ({response.status_code}): {response.text}")
            # 너무 많은 에러 발생 시 중단 방지
            time.sleep(1)

def migrate():
    if not os.path.exists(DB_PATH):
        print(f"로컬 DB({DB_PATH})를 찾을 수 없습니다. 먼저 create_local_db.py를 실행하세요.")
        return
        
    conn = sqlite3.connect(DB_PATH)
    
    # 1. population_age
    df_pop_age = pd.read_sql_query("SELECT * FROM population_age", conn)
    bulk_insert_to_supabase('population_age', df_pop_age)
    
    # 2. population_house
    df_pop_house = pd.read_sql_query("SELECT * FROM population_house", conn)
    bulk_insert_to_supabase('population_house', df_pop_house)
    
    # 3. region_code_mapping
    df_map = pd.read_sql_query("SELECT * FROM region_code_mapping", conn)
    bulk_insert_to_supabase('region_code_mapping', df_map)
    
    # 4. hospital_info
    df_hosp = pd.read_sql_query("SELECT * FROM hospital_info", conn)
    bulk_insert_to_supabase('hospital_info', df_hosp)
    
    # 5. hospital_specialty
    df_spec = pd.read_sql_query("SELECT * FROM hospital_specialty", conn)
    bulk_insert_to_supabase('hospital_specialty', df_spec)
    
    conn.close()
    print("마이그레이션이 모두 종료되었습니다.")

if __name__ == "__main__":
    migrate()
