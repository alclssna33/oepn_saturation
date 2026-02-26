"""
아파트(매매) 실거래가 Excel 파일들을 파싱하여
법정동별 평균/중위 평당가 테이블(apt_price_bjd)을 saturation.db에 저장합니다.

데이터 출처 : DB_data/아파트 실거래가/*.xlsx  (국토교통부 실거래가 공개시스템)
저장 테이블 : apt_price_bjd  (bjd_cd, avg_price_per_pyeong, med_price_per_pyeong,
                               trade_count, base_ym_from, base_ym_to)

실행 방법:
    python scripts/import_apt_price.py
"""

import sqlite3
import warnings
from pathlib import Path

import pandas as pd

warnings.simplefilter(action='ignore')

BASE_DIR = Path(__file__).parent.parent
DB_PATH  = BASE_DIR / 'data' / 'saturation.db'
APT_DIR  = BASE_DIR / 'DB_data' / '아파트 실거래가'

# 읍·면 접미사: 주소 중간 토큰에서 읍면 단위 식별·제거에 사용
_UB_MYEON = ('읍', '면')

# Excel 헤더는 13번째 행(0-index=12)에 위치 (위 12행은 안내문·검색조건)
_SKIPROWS = 12


# ─────────────────────────────────────────────────────────────────────────────
# 주소 파싱
# ─────────────────────────────────────────────────────────────────────────────
def _parse_sgg(text: str) -> tuple:
    """
    국토부 실거래가 '시군구' 컬럼 → (sido_raw, sigungu_raw, bjd_nm_raw)

    토큰 수별 처리 예시:
      2토큰: "세종특별자치시  다정동"               → ("세종특별자치시", "",               "다정동")
      3토큰: "서울특별시 서대문구 남가좌동"         → ("서울특별시",     "서대문구",         "남가좌동")
      4토큰: "충청북도 청주시 서원구 모충동"        → ("충청북도",       "청주시 서원구",    "모충동")
             "충청남도 논산시 연무읍 동산리"        → ("충청남도",       "논산시",           "동산리")
      5토큰: "경상북도 포항시 북구 흥해읍 옥성리"  → ("경상북도",       "포항시 북구",      "옥성리")

    규칙:
      - parts[0]  : 항상 시도명
      - parts[-1] : 항상 법정동/리명 (bjd_nm)
      - 중간 토큰 : 시군구 + 읍면(선택). 마지막 중간 토큰이 '읍'/'면'으로 끝나면 제거.
    """
    parts = str(text).split()
    if len(parts) < 2:
        return ('', '', '')
    sido   = parts[0]
    bjd_nm = parts[-1]
    middle = parts[1:-1]
    if middle and any(middle[-1].endswith(s) for s in _UB_MYEON):
        middle = middle[:-1]
    return (sido, ' '.join(middle), bjd_nm)


# ─────────────────────────────────────────────────────────────────────────────
# region_code_mapping 기반 bjd_cd lookup 빌드
# ─────────────────────────────────────────────────────────────────────────────
def _build_lookup(conn: sqlite3.Connection) -> tuple:
    """
    Returns:
        primary  : {(sido_nm, sigungu_nm, bjd_nm) → bjd_cd}  완전 일치용
        fallback : {(sido_nm, bjd_nm) → bjd_cd}               sigungu 무시 폴백
    """
    df = pd.read_sql_query(
        "SELECT DISTINCT sido_nm, sigungu_nm, bjd_nm, bjd_cd "
        "FROM region_code_mapping "
        "WHERE bjd_nm IS NOT NULL AND bjd_cd IS NOT NULL AND LENGTH(bjd_cd) = 10",
        conn,
    )
    df['sigungu_nm'] = df['sigungu_nm'].fillna('')

    dup_p = df.drop_duplicates(['sido_nm', 'sigungu_nm', 'bjd_nm'])
    primary = dict(zip(
        zip(dup_p.sido_nm, dup_p.sigungu_nm, dup_p.bjd_nm),
        dup_p.bjd_cd,
    ))

    dup_f = df.drop_duplicates(['sido_nm', 'bjd_nm'])
    fallback = dict(zip(
        zip(dup_f.sido_nm, dup_f.bjd_nm),
        dup_f.bjd_cd,
    ))
    return primary, fallback


# ─────────────────────────────────────────────────────────────────────────────
# bjd_cd 조인 (3단계 우선순위)
# ─────────────────────────────────────────────────────────────────────────────
def _join_bjd_cd(apt_df: pd.DataFrame, primary: dict, fallback: dict) -> pd.DataFrame:
    """
    1순위: (sido, sigungu, bjd_nm) 완전 일치
    2순위: (sido, sigungu 마지막 단어, bjd_nm)  — "청주시 서원구" → "서원구" 만으로 재시도
    3순위: (sido, bjd_nm)                        — sigungu 무시 폴백
    """
    # 1순위
    apt_df['bjd_cd'] = pd.Series(
        list(zip(apt_df.sido_raw, apt_df.sigungu_raw, apt_df.bjd_nm_raw)),
        index=apt_df.index,
    ).map(primary)

    # 2순위: sigungu 마지막 단어
    m1 = apt_df['bjd_cd'].isna()
    if m1.any():
        sg_last = apt_df.loc[m1, 'sigungu_raw'].str.rsplit(' ', n=1).str[-1].fillna('')
        fb1 = pd.Series(
            list(zip(apt_df.loc[m1, 'sido_raw'], sg_last, apt_df.loc[m1, 'bjd_nm_raw'])),
            index=apt_df.index[m1],
        ).map(primary)
        apt_df.loc[m1, 'bjd_cd'] = fb1

    # 3순위: sido + bjd_nm
    m2 = apt_df['bjd_cd'].isna()
    if m2.any():
        fb2 = pd.Series(
            list(zip(apt_df.loc[m2, 'sido_raw'], apt_df.loc[m2, 'bjd_nm_raw'])),
            index=apt_df.index[m2],
        ).map(fallback)
        apt_df.loc[m2, 'bjd_cd'] = fb2

    return apt_df


# ─────────────────────────────────────────────────────────────────────────────
# Excel 파일 로딩
# ─────────────────────────────────────────────────────────────────────────────
def _load_excel_files() -> pd.DataFrame:
    files = sorted(APT_DIR.glob('아파트(매매)_실거래가_*.xlsx'))
    if not files:
        raise FileNotFoundError(f"Excel 파일 없음: {APT_DIR}")

    frames = []
    for f in files:
        print(f"  {f.name} ...", end='', flush=True)
        df = pd.read_excel(f, skiprows=_SKIPROWS, header=0)
        # 필요 컬럼만 선택 (컬럼명 공백 제거)
        df.columns = df.columns.str.strip()
        need = ['시군구', '전용면적(㎡)', '거래금액(만원)', '계약년월']
        df = df[need].copy()
        # 계약년월: int/float → 순수 숫자 6자리 문자열 ("202501")
        df['계약년월'] = df['계약년월'].astype(str).str.split('.').str[0].str.strip()
        frames.append(df)
        print(f" {len(df):,}건")

    all_df = pd.concat(frames, ignore_index=True)
    print(f"\n  전체 로딩: {len(all_df):,}건")
    return all_df


# ─────────────────────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("아파트 실거래가 → apt_price_bjd 적재 시작")
    print("=" * 55)

    # ── 1. Excel 로딩 ─────────────────────────────────────────────────────────
    print(f"\n1. Excel 파일 로딩 ({APT_DIR.name}/) ...")
    apt_df = _load_excel_files()

    # ── 2. 전처리 ─────────────────────────────────────────────────────────────
    print("\n2. 전처리 (거래금액 정제, 평당가 계산, 주소 파싱) ...")

    # 거래금액: "120,000" → 120000.0
    apt_df['price'] = (
        apt_df['거래금액(만원)'].astype(str)
        .str.replace(',', '', regex=False)
        .pipe(pd.to_numeric, errors='coerce')
    )
    apt_df['area'] = pd.to_numeric(apt_df['전용면적(㎡)'], errors='coerce')
    apt_df = apt_df.dropna(subset=['price', 'area'])
    apt_df = apt_df[apt_df['area'] > 0].copy()

    # 평당가(만원/평): 1평 = 3.3058㎡
    apt_df['price_per_pyeong'] = apt_df['price'] * 3.3058 / apt_df['area']

    # 주소 파싱
    parsed = apt_df['시군구'].apply(_parse_sgg)
    apt_df[['sido_raw', 'sigungu_raw', 'bjd_nm_raw']] = pd.DataFrame(
        parsed.tolist(), index=apt_df.index
    )
    print(f"  유효 거래: {len(apt_df):,}건")

    # ── 3. bjd_cd 조인 ────────────────────────────────────────────────────────
    print("\n3. region_code_mapping 조인 (법정동 코드 부여) ...")
    conn = sqlite3.connect(DB_PATH)

    primary, fallback = _build_lookup(conn)
    apt_df = _join_bjd_cd(apt_df, primary, fallback)

    matched = apt_df['bjd_cd'].notna().sum()
    total   = len(apt_df)
    print(f"  매핑 성공: {matched:,} / {total:,} 건  ({matched / total * 100:.1f}%)")

    # 미매핑 샘플 (디버그용)
    unmatched = apt_df.loc[apt_df['bjd_cd'].isna(), '시군구'].drop_duplicates().head(15)
    if not unmatched.empty:
        print(f"\n  [미매핑 주소 샘플 — region_code_mapping에 법정동 없음]")
        for addr in unmatched:
            print(f"    {addr}")

    # ── 4. 법정동별 집계 ──────────────────────────────────────────────────────
    print("\n4. 법정동별 평당가 집계 ...")
    apt_valid = apt_df.dropna(subset=['bjd_cd']).copy()

    ym_vals      = sorted(apt_df['계약년월'].dropna().unique())
    base_ym_from = ym_vals[0]  if ym_vals else ''
    base_ym_to   = ym_vals[-1] if ym_vals else ''

    agg = (
        apt_valid.groupby('bjd_cd')['price_per_pyeong']
        .agg(
            avg_price_per_pyeong='mean',
            med_price_per_pyeong='median',
            trade_count='count',
        )
        .reset_index()
    )
    agg['avg_price_per_pyeong'] = agg['avg_price_per_pyeong'].round(0).astype(int)
    agg['med_price_per_pyeong'] = agg['med_price_per_pyeong'].round(0).astype(int)
    agg['base_ym_from'] = base_ym_from
    agg['base_ym_to']   = base_ym_to

    # 표시용 법정동 이름 조회
    bjd_names = pd.read_sql_query(
        "SELECT bjd_cd, "
        "       sido_nm || ' ' || COALESCE(sigungu_nm,'') || ' ' || bjd_nm AS full_nm "
        "FROM region_code_mapping "
        "WHERE bjd_cd IS NOT NULL "
        "GROUP BY bjd_cd",
        conn,
    )

    print(f"  집계 법정동 수: {len(agg):,}개  (기간: {base_ym_from} ~ {base_ym_to})")

    # ── 5. DB 저장 ────────────────────────────────────────────────────────────
    print("\n5. saturation.db → apt_price_bjd 저장 ...")
    agg.to_sql('apt_price_bjd', conn, if_exists='replace', index=False)
    cur = conn.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_apt_bjd_cd ON apt_price_bjd(bjd_cd)")
    conn.commit()
    conn.close()

    # ── 6. 요약 출력 ──────────────────────────────────────────────────────────
    print("\n" + "=" * 55)
    print(f"완료!  {len(agg):,}개 법정동 평당가 저장됨")
    print(f"기간:  {base_ym_from} ~ {base_ym_to}")
    print("-" * 55)

    agg_display = agg.merge(bjd_names, on='bjd_cd', how='left').drop_duplicates('bjd_cd')

    print("[전체 평당가 분포]")
    print(f"  전국 법정동 평균  : {agg['avg_price_per_pyeong'].mean():>9,.0f} 만원/평")
    print(f"  전국 법정동 중위  : {agg['med_price_per_pyeong'].median():>9,.0f} 만원/평")

    print("\n  평균 평당가 TOP 5:")
    top5 = agg_display.nlargest(5, 'avg_price_per_pyeong')
    for _, r in top5.iterrows():
        nm = r.get('full_nm', r['bjd_cd'])
        print(f"    {nm:<30s} {r['avg_price_per_pyeong']:>8,} 만원/평  ({r['trade_count']}건)")

    print("\n  평균 평당가 BOTTOM 5:")
    bot5 = agg_display.nsmallest(5, 'avg_price_per_pyeong')
    for _, r in bot5.iterrows():
        nm = r.get('full_nm', r['bjd_cd'])
        print(f"    {nm:<30s} {r['avg_price_per_pyeong']:>8,} 만원/평  ({r['trade_count']}건)")

    print("=" * 55)


if __name__ == "__main__":
    main()
