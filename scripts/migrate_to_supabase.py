"""
로컬 SQLite DB → Supabase(PostgreSQL) 마이그레이션 스크립트

사전 조건:
  1. pip install psycopg2-binary
  2. .env 파일에 SUPABASE_DB_URL 설정
     SUPABASE_DB_URL=postgresql://postgres:[password]@db.[ref].supabase.co:5432/postgres
  3. Supabase SQL Editor에서 DB_data/supabase_schema.md 의 DDL 실행 완료

실행:
  python scripts/migrate_to_supabase.py
"""

import os
import sqlite3
import sys

import pandas as pd

# ── 환경 변수 로드 ──────────────────────────────────────────────────────────
def _load_env():
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    env_vars = {}
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, val = line.split('=', 1)
                    env_vars[key.strip()] = val.strip()
    return env_vars

env = _load_env()
SUPABASE_DB_URL = env.get('SUPABASE_DB_URL', '')

if not SUPABASE_DB_URL:
    print("오류: .env 파일에 SUPABASE_DB_URL을 설정해주세요.")
    print("예시:")
    print("  SUPABASE_DB_URL=postgresql://postgres:[password]@db.[ref].supabase.co:5432/postgres")
    print()
    print("Supabase 대시보드 → Settings → Database → Connection string → URI 에서 복사하세요.")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'saturation.db')

# 마이그레이션 대상 테이블 (순서 중요: FK 참조 순)
TABLES = [
    "population_age",
    "population_house",
    "region_code_mapping",
    "hospital_info",
    "hospital_specialty",
    "apt_price_bjd",
]


def _bulk_insert(pg_conn, table_name: str, df: pd.DataFrame, batch_size: int = 500):
    """psycopg2 execute_values를 사용한 고속 bulk INSERT"""
    from psycopg2.extras import execute_values

    if df.empty:
        print(f"  [{table_name}] 데이터 없음 — 건너뜀")
        return

    # NaN → None 변환
    df = df.where(pd.notnull(df), None)
    cols = list(df.columns)
    col_str = ", ".join(f'"{c}"' for c in cols)

    total = len(df)
    inserted = 0

    with pg_conn.cursor() as cur:
        for i in range(0, total, batch_size):
            batch = df.iloc[i : i + batch_size]
            rows = [tuple(row) for row in batch.itertuples(index=False, name=None)]
            sql = f'INSERT INTO {table_name} ({col_str}) VALUES %s ON CONFLICT DO NOTHING'
            execute_values(cur, sql, rows)
            inserted += len(rows)
            print(f"  [{table_name}] {inserted}/{total} 건 삽입 완료")

    pg_conn.commit()
    print(f"  [{table_name}] 완료 ({total}건)")


def migrate():
    if not os.path.exists(DB_PATH):
        print(f"로컬 DB({DB_PATH})를 찾을 수 없습니다.")
        print("먼저 python scripts/create_local_db.py 를 실행하세요.")
        sys.exit(1)

    try:
        import psycopg2
    except ImportError:
        print("psycopg2가 설치되지 않았습니다.")
        print("pip install psycopg2-binary 를 실행한 뒤 다시 시도하세요.")
        sys.exit(1)

    print(f"Supabase 연결 중... ({SUPABASE_DB_URL[:40]}...)")
    try:
        pg_conn = psycopg2.connect(SUPABASE_DB_URL)
    except Exception as e:
        print(f"Supabase 연결 실패: {e}")
        print("SUPABASE_DB_URL 및 네트워크 연결을 확인하세요.")
        sys.exit(1)

    print("연결 성공. 로컬 SQLite DB 읽는 중...")
    sqlite_conn = sqlite3.connect(DB_PATH)

    try:
        for table in TABLES:
            print(f"\n[{table}] 처리 시작")

            # 테이블 존재 여부 확인
            chk = pd.read_sql_query(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'",
                sqlite_conn,
            )
            if chk.empty:
                print(f"  [{table}] 로컬 DB에 없음 — 건너뜀")
                continue

            df = pd.read_sql_query(f"SELECT * FROM {table}", sqlite_conn)
            print(f"  [{table}] 로컬 {len(df)}건 읽음. Supabase에 삽입 중...")

            # 기존 데이터 삭제 후 재삽입
            with pg_conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
            pg_conn.commit()

            _bulk_insert(pg_conn, table, df)

    except Exception as e:
        pg_conn.rollback()
        print(f"\n오류 발생: {e}")
        raise
    finally:
        sqlite_conn.close()
        pg_conn.close()

    print("\n마이그레이션이 모두 완료되었습니다.")
    print("이제 .env 파일에 SUPABASE_DB_URL이 설정된 상태로 앱을 실행하면 Supabase를 사용합니다.")
    print("  streamlit run app.py")


if __name__ == "__main__":
    migrate()
