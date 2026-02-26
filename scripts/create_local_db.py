import pandas as pd
import sqlite3
import os
import warnings

# 경고 무시
warnings.simplefilter(action='ignore')

# 원본 데이터 경로
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
DB_DATA_DIR = os.path.join(BASE_DIR, 'DB_data')
DB_PATH = os.path.join(DATA_DIR, 'saturation.db')

def create_local_db():
    print(f"[{'='*40}]")
    print(f"로컬 SQLite DB 구축 시작: {DB_PATH}")
    print(f"[{'='*40}]")
    
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # =====================================================================
        # 1. 법정동-행정동 매핑 데이터 (region_code_mapping)
        # =====================================================================
        print("\n1. 코드 매핑 데이터 적재 중...")
        kikmix_path = os.path.join(DB_DATA_DIR, '법정동_행정동_코드맵핑_테이블', 'KIKmix.20260201.xlsx')
        if os.path.exists(kikmix_path):
            df_map = pd.read_excel(kikmix_path, dtype=str)
            # 행정동코드 10자리 그대로 사용, 법정동코드 10자리
            df_map = df_map[['행정동코드', '시도명', '시군구명', '읍면동명', '법정동코드', '동리명']]
            df_map.columns = ['hjd_cd', 'sido_nm', 'sigungu_nm', 'dong_nm', 'bjd_cd', 'bjd_nm']
            df_map.to_sql('region_code_mapping', conn, if_exists='replace', index=False)
            print(f" - 적용 완료: {len(df_map)}행")
        
        # =====================================================================
        # 2. 인구 데이터 적재 (population_data -> pop_age, pop_house)
        # =====================================================================
        print("\n2. 인구 데이터 적재 중...")
        age_pop_path = os.path.join(DB_DATA_DIR, '연령별인구현황(월별).xlsx')
        if os.path.exists(age_pop_path):
            print(" - 연령별 인구수 처리 중...")
            df_age = pd.read_excel(age_pop_path, skiprows=3)
            # 대상: 행정기관코드, 행정기관, 총인구수, 0~9세... 100세이상
            age_cols = ['행정기관코드', '행정기관', '총 인구수', '0~9세', '10~19세', '20~29세', '30~39세', 
                        '40~49세', '50~59세', '60~69세', '70~79세', '80~89세', '90~99세', '100세 이상']
            
            df_age = df_age[age_cols].copy()
            # 컴마 제거 및 정수형 변환
            for col in age_cols[2:]:
                df_age[col] = df_age[col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')
            
            df_age.rename(columns={
                '행정기관코드': 'adm_cd',
                '행정기관': 'adm_nm',
                '총 인구수': 'total_pop',
                '0~9세': 'age_0_9', '10~19세': 'age_10_19', '20~29세': 'age_20_29',
                '30~39세': 'age_30_39', '40~49세': 'age_40_49', '50~59세': 'age_50_59',
                '60~69세': 'age_60_69', '70~79세': 'age_70_79', '80~89세': 'age_80_89',
                '90~99세': 'age_90_99', '100세 이상': 'age_100_plus'
            }, inplace=True)
            
            df_age['adm_cd'] = df_age['adm_cd'].astype(str)
            df_age.to_sql('population_age', conn, if_exists='replace', index=False)
            print(f" - [연령 인구] 적용 완료: {len(df_age)}행")

        house_pop_path = os.path.join(DB_DATA_DIR, '인구및세대현황(월별).xlsx')
        if os.path.exists(house_pop_path):
            print(" - 세대수 처리 중...")
            df_house = pd.read_excel(house_pop_path, skiprows=3)
            # 대상: 행정기관코드, 행정기관, 총인구수, 세대수, 남자인구수, 여자인구수
            # 컬럼 인덱스 0, 1, 2, 3, 4, 5
            df_house = df_house.iloc[:, 0:6].copy()
            df_house.columns = ['adm_cd', 'adm_nm', 'total_pop', 'households', 'male_pop', 'female_pop']
            
            for col in ['total_pop', 'households', 'male_pop', 'female_pop']:
                df_house[col] = df_house[col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce').fillna(0).astype('int64')

            df_house['adm_cd'] = df_house['adm_cd'].astype(str)
            df_house.to_sql('population_house', conn, if_exists='replace', index=False)
            print(f" - [세대 인구] 적용 완료: {len(df_house)}행")

        # =====================================================================
        # 3. 병의원 데이터 적재 (hospital_info)
        # =====================================================================
        print("\n3. 병원 기본 정보 적재 중...")
        hosp_info_path = os.path.join(DB_DATA_DIR, '전국 병의원 및 약국 현황 2025.12', '1.병원정보서비스(2025.12.).xlsx')
        if os.path.exists(hosp_info_path):
            df_hosp = pd.read_excel(hosp_info_path, dtype=str)
            hosp_cols = {
                '암호화요양기호': 'ykiho',
                '요양기관명': 'hosp_nm',
                '종별코드': 'cl_cd',
                '종별코드명': 'cl_cd_nm',
                '시도코드': 'sido_cd',
                '시군구코드': 'sigungu_cd',
                '읍면동': 'emdong_nm',
                '주소': 'addr',
                '개설일자': 'estb_dd',
                '총의사수': 'dr_tot_cnt',
                '좌표(X)': 'x_pos',
                '좌표(Y)': 'y_pos'
            }
            df_hosp = df_hosp[list(hosp_cols.keys())].rename(columns=hosp_cols)
            df_hosp['estb_dd'] = df_hosp['estb_dd'].str.replace('-', '') # 숫자형식 통일 (ex: 20240101)
            df_hosp.to_sql('hospital_info', conn, if_exists='replace', index=False)
            print(f" - 적용 완료: {len(df_hosp)}행")

        # =====================================================================
        # 4. 진료과목 데이터 적재 (hospital_specialty)
        # =====================================================================
        print("\n4. 병원 진료과목 데이터 적재 중...")
        hosp_spec_path = os.path.join(DB_DATA_DIR, '전국 병의원 및 약국 현황 2025.12', '5.의료기관별상세정보서비스_03_진료과목정보 2025.12..xlsx')
        if os.path.exists(hosp_spec_path):
            df_spec = pd.read_excel(hosp_spec_path, dtype=str)
            spec_cols = {
                '암호화요양기호': 'ykiho',
                '진료과목코드': 'dgsbjt_cd',
                '진료과목코드명': 'dgsbjt_cd_nm',
                '과목별 전문의수': 'dr_cnt'
            }
            df_spec = df_spec[list(spec_cols.keys())].rename(columns=spec_cols)
            df_spec.to_sql('hospital_specialty', conn, if_exists='replace', index=False)
            print(f" - 적용 완료: {len(df_spec)}행")

        # 인덱스 생성
        print("\n5. DB 인덱스 생성 중...")
        cur = conn.cursor()
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pop_age_adm_cd ON population_age(adm_cd)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pop_house_adm_cd ON population_house(adm_cd)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_hosp_ykiho ON hospital_info(ykiho)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_map_hjd ON region_code_mapping(hjd_cd)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_spec_ykiho ON hospital_specialty(ykiho)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_spec_dgsbjt ON hospital_specialty(dgsbjt_cd)")
        conn.commit()

    except Exception as e:
        print(f"오류 발생: {e}")
    finally:
        conn.close()
        print("\n작업 완료.")

if __name__ == "__main__":
    create_local_db()
