## 📋 [개원포화도 프로젝트] Project Guide

### 1. 프로젝트 개요

* **목적:** 행정구역별(시군구/행정동) 인구 데이터와 병의원 현황 데이터를 결합하여 전공과목별 '개원 포화도 지수'를 산출하고 시각화함.
* **대상 유저:** 개원 입지를 고민하는 의사(개비공 커뮤니티 회원).
* **주요 기술 스택:** Python, Streamlit (UI), Pandas (데이터 분석), Plotly/Mapbox (지도 시각화).

---

### 2. 데이터 수집 전략 및 DB 아키텍처 개편 (Data Acquisition & Architecture)

**[개편 배경]** 
기존에는 매 요청마다 행안부/심평원 API를 실시간으로 호출하여 데이터를 수집했으나, 응답 지연과 데이터 정확성 등 문제가 발생했습니다.
이를 해결하기 위해 **데이터베이스(DB) 기반 로컬/온라인 저장소 체계**로 개편합니다.
*   **현재 진행 상태:** 로컬 파일(`DB_data/` 폴더 내 엑셀/텍스트 데이터)을 기반으로 DB를 구성하고, API는 향후 **DB 주기적 업데이트 용도**로만 사용합니다.
*   **온라인 DB 마이그레이션:** 로컬에서 구성한 DB를 최종적으로 **Supabase**로 이관하여 안정적인 데이터 서빙 환경을 구축할 계획입니다.

설계를 위해 `GUIDE` 폴더의 문서를 적극 참조합니다.
* **공공데이터포털 API 인증키:** `GUIDE/공공데이터포탈 API키.txt` 참조 (개인 인증키 활용 - 향후 DB 업데이트 스크립트 용).

#### A. 인구 데이터 (이중 API 구조)

| API | URL | 제공 데이터 |
|-----|-----|------------|
| RDOA (행안부) | `rdoa.jumin.go.kr/openStats` | 총인구수, 세대수, 성별 인구 |
| JUMIN (행안부) | `jumin.mois.go.kr` | 연령대별 인구 (10세 단위) |

* **관련 가이드:** `GUIDE/행정안전부_행정동별(통반단위) 주민등록 인구 및 세대현황 가이드.docx`
* **⚠️ JUMIN 서버 주의사항:** 연속 요청 시 `RemoteDisconnected` 발생. **최대 3회 재시도 + 지수 백오프(1→2→4초) + 세션 재생성** 로직으로 대응. sido 레벨 루프에서는 구(區) 간 0.8초 딜레이 추가 필요.

#### B. 병의원 데이터 (심평원 HIRA Open API)

* **데이터 소스:** `apis.data.go.kr/B551182/hospInfoServicev2/getHospBasisList`
* **가이드 문서:** `GUIDE/OpenAPI활용가이드_건강보험심사평가원(의료기관별상세정보서비스).docx`
* **⚠️ clCd 파라미터 주의:** HIRA API는 `clCd`에 **단일값만 지원**. 콤마 구분 다중값(`"31,21,11"`) 전달 시 0건 반환됨. 단일 값이면 전달, 복수면 파라미터 생략하여 전체 종별 반환.
* **⚠️ API 오류 감지:** HTTP 200이어도 XML 내부에 `resultCode`가 있을 수 있음. `_parse_response`에서 `resultCode != "00"` 체크로 오류 감지.

---

### 3. 데이터 처리 및 지수 산출 로직

#### A. 포화도 지수 ($SI$, Saturation Index) 정의

단순 인구 대비 병원 수가 아닌, **타겟 인구 가중치**를 적용한 지수를 산출합니다.

1.  **일반 포화도:** $SI_{general} = \frac{\text{해당 구역 총 인구수}}{\text{해당 구역 특정과 의원 수}}$
2.  **세대 포화도:** $SI_{household} = \frac{\text{해당 구역 총 세대수}}{\text{해당 구역 특정과 의원 수}}$
3.  **타겟 인구 포화도:** $SI_{target} = \frac{\text{해당 구역 타겟 연령층 인구수}}{\text{해당 구역 특정과 전문의 총 인원수}}$

*   **블루오션 가중치**: 의원이 0개인 지역은 $SI = \infty$로 처리(코드상 3.0)되어 지도에서 가장 진한 초록색('기회 최대')으로 표시됩니다.

#### B. match_key 생성 규칙

병원 데이터(`adm_cd2`, GeoJSON 기반)와 인구 데이터(`admmCd`) 간의 키 통일 방식:

| 분석 레벨 | match_key 형식 | 예시 |
|-----------|----------------|------|
| national | 시도코드 앞 2자리 | `"11"` (서울) |
| sido | **시(市) 레벨** 5자리 (구가 있는 시는 구→시 통합) | `"41110"` (수원시), `"11215"` (서울 광진구) |
| dong | 전체 행정동코드 10자리 | `"1111051500"` |

특수 코드 변환: `adm_cd2`가 "42"로 시작 → "51"(강원), "45"로 시작 → "52"(전북), "41000..."으로 시작 → "36"(세종)

**sido 레벨 구→시 통합 로직** (`existing_sgg_codes` 체크):
- `adm[:4]+"0"` (city_5)가 `existing_sgg_codes` 집합에 존재 → 하위 구이므로 city_5 반환
  - 수원시 장안구 "41111" → city_5 "41110" → "41110" in sgg_codes(수원시) → "41110" ✓
  - 부천시 오정구 "41192" → city_5 "41190" → "41190" in sgg_codes(부천시) → "41190" ✓
- city_5가 `existing_sgg_codes`에 없으면 → 독립 구이므로 원래 5자리 유지
  - 서울 광진구 "11215" → city_5 "11210" → not in sgg_codes → "11215" 유지 ✓

---

### 4. 개발 단계별 진행 현황

- [x] **1단계 ~ 5단계 (완료):** 기초 인프라 및 MVP 구축
  - API 인증키 관리 설정, 인구/병원 API 모듈화, 데이터 병합 및 Streamlit UI 기본 구성 완료.

- [x] **6단계 ~ 9단계 (완료):** 버그 수정 및 전국 확장 기반 마련
  - 포화도 등급 반전(높은 SI=초록), 지도 클릭 이벤트 연동, 전국 GeoJSON 도입 및 HIRA API Fallback 로직 강화.
  - **심평원 독자 코드 체계 발견**: 행안부 표준 코드와 다른 심평원만의 시도/시군구 코드를 전수 조사하여 매핑 테이블 구축.

- [x] **10단계 (완료):** 계층적 거시 분석 체계 도입 (전국 및 시도 전체)
  - **National/Sido 레벨**: 17개 시도별 비교 및 특정 시도 내 구별 비교 기능 추가.
  - **동적 GeoJSON Dissolve**: 행정동 경계를 실시간으로 병합하여 시도/구 단위의 깔끔한 지도 시각화 구현.

- [x] **11단계 (완료):** UI/UX 감성 품질 고도화
  - **로딩 UI 혁신**: 구글 드라이브 이미지 + CSS 스피너 애니메이션 + 안내 문구.
  - **폰트 및 아이콘 보정**: Material Symbols 텍스트 겹침 버그를 CSS Ligature 설정으로 완전 해결.
  - **지도 제어 강화**: 확대/축소 버튼 및 휠 줌 활성화.

- [x] **12단계 (완료):** 데이터 정밀도 및 타입 안정성 확보
  - **동 단위 전수 합산**: API의 불안정한 상위 집계 대신 하위 데이터를 모두 모아 합산하는 방식으로 정확도 100% 달성.
  - **숫자 타입 강제**: Pandas의 Object 타입 인식 오류를 `int64` 캐스팅 및 `pd.to_numeric`으로 원천 차단.

- [x] **13단계 (완료):** 운영 보안 (관리자 모드)
  - **전국 분석 보호**: 고비용 API 호출인 전국 단위 분석 시 관리자 비밀번호(`ADMIN_PASSWORD`) 요구 로직 추가.

- [x] **14단계 (완료):** 행정구역 코드 표준화 및 최종 안정화
  - **강원/전북/세종 특수 처리**: 신설된 특별자치도 코드(51, 52, 36)와 구코드(42, 45, 41) 간의 정합성을 `_get_match_key` 및 `_standardize_code`로 완벽히 해결.
  - **동적 컬럼 파싱**: API 레벨별로 달라지는 테이블 구조를 역방향 인덱싱으로 극복하여 데이터 누락 방지.

- [x] **15단계 (완료):** 중요 버그 수정 및 데이터 정확성 복구
  - **[버그] DataFrame apply 오류**: geopandas sjoin 이후 `hosp_mapped`에 중복 컬럼 발생 시 `apply(func, axis=1)`이 Series 대신 DataFrame을 반환하는 pandas 버그. `loc[:, ~columns.duplicated()]`로 사전 제거하여 해결.
  - **[버그] specialty_cd 컬럼 누락**: `h_frames`가 빈 경우 `pd.DataFrame()`(컬럼 없음)으로 폴백되어 이후 groupby에서 `KeyError`. `HOSPITAL_COLUMNS` 상수를 `hospital_api.py`에 정의하고 빈 폴백 시 `pd.DataFrame(columns=HOSPITAL_COLUMNS)` 사용으로 해결.
  - **[버그] HIRA API clCd 다중값 미지원**: `clCd=31,21,11` 전달 시 0건 반환. 단일값일 때만 파라미터 전달, 복수 시 생략하도록 수정. 동시에 `_parse_response`에 `resultCode` 체크로 API 오류 가시화.
  - **[버그] dgsbjtCd 코드 오기입**: `"18": 재활의학과`, `"22": 가정의학과`, `"24": 신경과`가 전부 잘못된 HIRA 코드. 실제 API 기준으로 전면 교정 (`재활의학과→21`, `가정의학과→23`, `신경과→02`).
  - **[버그] JUMIN 서버 연결 끊김**: sido 레벨(서울 전체 등) 분석 시 25개 구 × 2회 = 50+ 연속 POST 요청으로 `RemoteDisconnected` 발생. `_reset_jumin_session()` + 지수 백오프 재시도 + 구 간 딜레이 0.8초 추가로 해결.

- [x] **16단계 (완료):** UI 대폭 개선
  - **지도 스크롤 줌**: `config={"scrollZoom": True}` + `uirevision="map-view"`로 줌 후 지도 위치 유지.
  - **포화도 막대그래프 추가**: 지도 우측(40%)에 행정동별 포화도 순위 막대그래프 추가. 기회 최대(∞) 지역은 맨 위 별도 표시.
  - **지도 클릭 → 의원 목록**: `on_select="rerun"` + `clickmode="event+select"` 조합으로 지역 클릭 시 해당 행정동의 의원 상세 목록(의원명, 종별, 주소, 전문의수, 의사수) 표시. 등급 배지 + 닫기 버튼 포함.
  - **요약 지표 카드**: 과목별 총 의원 수 / 분석 행정동 수 / 평균 포화도 / 기회 지역 수 표시.
  - **로딩 스피너**: CSS `@keyframes` 애니메이션 링 스피너 + 이미지 + 안내 문구.
  - **디버그 패널**: 기본 접힘(expanded=False)으로 변경.

- [x] **17단계 (완료):** 진료과목 코드 전면 확장
  - **HIRA API 공식 코드표 기준** (`GUIDE/OpenAPI활용가이드...docx` 참조) 10개 → 34개로 확장.
  - 의과 20종 / 치과 6종(치과, 교정과, 소아치과, 치주과, 보존과, 통합치의학과) / 한방 8종(한방내과, 침구과, 한방재활의학과, 사상체질과 등) 추가.

- [x] **18단계 (완료):** UI/UX 세부 개선 및 병원 상세 팝업 도입
  - **호버 툴팁 강화**: 지도 위 커서 호버 시 기존 포화도·의원수에 더해 총 인구수·세대수 추가 표시.
  - **클릭 요약 지표 확장**: 행정동 클릭 시 하단 요약 카드를 3개 → 5개(의원수·전문의수·포화도·총인구수·세대수)로 확장.
  - **병원 상세 팝업 (`@st.dialog`)**: 의원 목록에서 [상세] 버튼 클릭 시 모달 팝업 표시.
    - 표시 항목: 의료기관명, 종별, 주소, **진료과목 전체** (같은 `ykiho`의 모든 `specialty_nm`을 배지로 표시), 의사 총수, 전문의 수, 개설일자(`estbDd`).
    - 개설일자 포맷: `"20050312"` → `"2005-03-12"` 자동 변환.
    - 네이버 지도 바로가기 버튼 (검색 쿼리: 병원명 + 시도명 + 동명 — 전체 주소는 검색 실패 발생).
  - **`hospital_api.py` 컬럼 확장**: `HOSPITAL_COLUMNS` 및 `_KEEP` 리스트에 `"estbDd"` 추가. 캐시 초기화 후 재분석 시 개설일자 표시.
  - **한방 과목 제거**: 진료과목 선택란에서 한방 8종(한방내과·한방부인과·한방소아과·한방안이비인후피부과·한방신경정신과·침구과·한방재활의학과·사상체질과) 제거. 현재 선택 가능 과목: 의과 20종 + 치과 6종.
  - **초록 버튼 테마**: 분석 실행·캐시 초기화 등 primary 버튼, 네이버 지도 link 버튼, multiselect 선택 태그를 `#16A34A` 초록 계열로 통일. `* {}` 자식 선택자로 내부 `<p>` 색상 덮어쓰기 문제 해결.
  - **지도·막대그래프 색상 통일**: 기존 지도의 연속 컬러스케일(빨강→주황→연두→초록)을 `saturation_level` 기반 이산 4색으로 교체. 포화=`#DC2626`, 보통=`#D97706`, 여유=`#16A34A`, 데이터없음=`#9CA3AF`. 지도 colorbar에 등급명 레이블 표시.
  - **막대그래프 평균 기준선**: x=1 위치에 점선 세로선 + "평균(1.0)" 레이블 추가. 기준선 좌측=포화, 우측=여유를 직관적으로 파악 가능.

- [x] **19단계 (완료):** 데이터베이스(DB) 기반 아키텍처 개편 및 최적화
  - **Local SQLite DB 구축**: `DB_data` 폴더의 원본 엑셀(인구수, 병의원, 법정동 맵핑 정보 등 50만여 건)을 파싱하여 정규화된 5개 RDB 테이블(`data/saturation.db`)로 구성 완료.
  - **Streamlit 성능 혁신**: 기존의 느린 심평원/행안부 HTTP API 호출(`population_api.py`, `hospital_api.py`) 로직을 Pandas `read_sql_query` 기반 SQLite 즉시 조회로 전환. 화면 지도 및 그래프 렌더링 속도 수 십 배 상승.
  - **Supabase(온라인 DB) 통합 준비**: 언제든 퍼블릭 클라우드 DB로 이관할 수 있도록 `scripts/migrate_to_supabase.py` 스크립트 작성 및 `DB_data/supabase_schema.md` DDL 구축.
  - **정기 업데이트 스크립트 도입**: 로컬 및 원격 DB의 데이터를 스케줄러를 통해 최신 공공데이터와 동기화할 수 있도록 `scripts/update_db_from_api.py` 파일 생성.

- [x] **21단계 (완료):** 아파트 실거래가(소득 대리 지표) 파이프라인 구축 및 UI 연동
  - **데이터 수집**: 국토교통부 실거래가 Excel 14개월치(2025.01~2026.02, 607,586건)를 `DB_data/아파트 실거래가/` 폴더에 적재.
  - **`scripts/import_apt_price.py` 신규 작성**:
    - Excel skiprows=12 구조 파싱, 거래금액(쉼표 포함 문자열) 정제, 평당가 계산 (`거래금액(만원) × 3.3058 / 전용면적(㎡)`).
    - `시군구` 컬럼 토큰 파싱(2~5토큰 대응): `parts[0]`=시도, `parts[-1]`=법정동명, 중간 읍면 토큰 제거 로직.
    - 3단계 Fallback join: ① (시도+시군구+법정동) → ② (시도+시군구 마지막 단어+법정동) → ③ (시도+법정동) 순 매핑, **매핑 성공률 100%** 달성.
    - **3,452개 법정동** 집계 결과를 `apt_price_bjd` 테이블로 저장 (avg/med 평당가, 거래건수, 기간).
  - **`modules/data_merge.py` — `enrich_with_apt_price()` 함수 신규 추가**:
    - `hjd_cd → bjd_cd → apt_price_bjd` 거래량 가중 평균 조인 (SQL 단계에서 처리).
    - dong/sido/national 레벨별 match_key 길이(10/5/2자리)에 따라 집계 자동 분기.
    - `apt_price_bjd` 테이블 미존재 시 graceful fallback — 원본 DataFrame 그대로 반환.
    - `DataMerger.run()` 결과 산출 직후 자동 호출로 모든 과목 결과에 `avg_price_per_pyeong` 컬럼 추가.
  - **`app.py` UI 연동**:
    - `_make_scatter_chart()` 신규: 아파트 평당가(X) × 포화도 지수(Y) 산점도. 중위 평당가/SI=1.0 기준선, 사분면 레이블(최적 입지·기회 지역·주의·불리) 오버레이. 지도+막대 섹션 하단에 자동 표시.
    - 지도 hover tooltip에 아파트 평당가 항목 조건부 추가.
    - 행정동 클릭 요약 지표: 5개 → 6개 (아파트 평당가 추가).

- [x] **20단계 (완료):** sido 분석 match_key 버그 수정 — 구(區) 분할 시·부천시 코드 불일치 해결
  - **[버그] 구분할 시의 시군구 분리 표시**: `get_sgg_list`가 시(市) 레벨과 하위 구(區) 레벨 항목을 함께 반환해, 수원시가 4개 구로 쪼개지는 등 같은 시가 별도 폴리곤으로 표시되는 문제 발견.
    - 전국 56개 코드 영향: 수원시(4구)·성남시(3구)·안양시(2구)·부천시(3구)·고양시(3구)·용인시(3구)·천안시(2구)·청주시(4구)·창원시(5구) 등.
  - **[버그] 부천시 의원 0개**: 구폐지(2016) 후에도 population DB는 구 코드(41192/41194/41196)를 유지하는 반면, GeoJSON은 통합 코드(41190)만 사용 → hospital match_key `41190`이 population match_key `4119x`와 불일치 → 의원 0개 오표시.
  - **[해결책] `existing_sgg_codes` 체크 로직 도입** (`modules/data_merge.py`):
    - `get_sgg_list` 결과로 시도 내 전체 sgg 코드 집합(`existing_sgg_codes`) 빌드.
    - population 루프 및 `_finalize_key` 함수에서 `city_5 = adm[:4]+"0"`이 `existing_sgg_codes`에 존재하면 → 하위 구이므로 parent city 코드로 통합; 없으면 → 독립 구이므로 원래 코드 유지.
    - 서울 광진구(11215 → city_5 11210 not in sgg_codes → 유지), 수원시 장안구(41111 → city_5 41110 in sgg_codes → 통합) 등 모든 케이스 정확 처리.
    - `pd.concat` 후 동일 match_key 행을 수치 컬럼 기준 합산하여 구별 인구를 시 단위로 통합.
  - **[해결책] GeoJSON dissolve_key 동적 계산** (`app.py`):
    - 기존 단순 `str[:5]` 슬라이싱 → `existing_sgg_codes` 기반 동적 판별로 교체, 구 폴리곤을 parent city 폴리곤으로 정확히 병합.

- [x] **22단계 (완료):** 개비공 소득지수 도입 (아파트 평당가 기반 복합 지표)
  - **배경**: 아파트 평당가를 단순 수치로 표시하는 대신, '개비공만의 행정동 소득수준 지표'로 가공하여 직관적인 등급 비교 가능하게 함.
  - **지표 설계**: 아파트 평당가(70%) + 30~59세 인구비율(30%) 복합 → log2 로그 변환 후 전국 기준 분위수 → S/A/B/C/D 5등급.
  - **`modules/data_merge.py` — `calc_income_index()` 함수 신규 추가**:
    - `import numpy as np` 추가.
    - `avg_price_per_pyeong`의 log2 로그 정규화 → 전국 min-max 스케일링(평당가 고왜도 분포 보정).
    - `population_age` 테이블에서 30~59세 비율 산출 후 전국 min-max 스케일링.
    - 복합 점수(`composite = price_score×0.7 + active_score×0.3`) 전국 quintile로 5등급 분류: S(80%↑) / A(60~80%) / B(40~60%) / C(20~40%) / D(0~20%).
    - 아파트 데이터 없는 지역은 `income_grade=None` (UI에서 회색 미산출 표시).
    - `DataMerger.run()`에서 `enrich_with_apt_price()` 직후 자동 체이닝 호출.
  - **`app.py` UI 연동**:
    - 지도 hover tooltip에 "개비공 소득지수: S/A/B/C/D" 조건부 표시.
    - `_make_scatter_chart()` X축: 평당가(수치) → `income_score`(0-100점)로 교체, 레이블 "← 저소득 | 개비공 소득점수 (0-100) | 고소득 →", 중간선 X=50.
    - 행정동 클릭 요약 카드 6번째 지표: 소득지수 등급 컬러 배지 (S=보라`#7C3AED` / A=파랑`#2563EB` / B=초록`#16A34A` / C=주황`#D97706` / D=회색`#9CA3AF`).
    - 산점도 마커 크기 확대: `size=8 → 12`, `line_width=0.5 → 0.8`.
  - **검증**: 전국 3,925개 지역에 S/A/B/C/D 각 785개씩 균등 분포 확인.

- [x] **23단계 (완료):** 의원 위치 핀 표시 기능 추가
  - **기능**: 행정동 분석 지도에서 특정 행정동 클릭 시, 해당 동 내 의원 위치를 파랑 원형 마커(`#1D4ED8`)로 오버레이 표시.
  - **토글 조건**: `analysis_level == "dong"` (특정 시군구 선정 상태)에서만 "📌 의원 위치 핀 표시" 토글 버튼 노출. sido/national 레벨에서는 토글 미표시.
  - **표시 조건**: 토글 ON + 행정동 선택 완료 + XPos/YPos 유효값 보유 의원만 표시.
  - **`_make_choropleth()` 변경사항** (`app.py`):
    - `hospital_markers: pd.DataFrame | None = None` 파라미터 추가 → `go.Scattermapbox` 트레이스 레이어로 의원 마커 오버레이.
    - `selected_key: str = ""` 파라미터 추가 → 이중 Choroplethmapbox 레이어 구현.
    - 이중 레이어: ① 전체 행정동 흐림(opacity=0.2, 클릭 가능 유지) + ② 선택된 행정동 강조(opacity=0.88, line_width=2.5).
    - 행정동 미선택 상태: 단일 레이어(opacity=0.78) 정상 표시.
  - **버그 수정 3건**:
    1. **`Scattermapbox marker.line` 미지원**: `go.Scattermapbox` Marker에 `line` 속성 없음 → ValueError 발생. 해결: `line=dict(...)` 제거.
    2. **`symbol="marker"` 렌더링 실패**: Maki 아이콘은 Mapbox 토큰 없이 carto-positron 스타일에서 렌더링 불가 → 트레이스 전체 무표시. 해결: `symbol` 파라미터 제거, 기본 circle 사용.
    3. **Scattermapbox 클릭 시 행정동 선택 해제**: 마커 클릭 시 `"location"` 키 없어 `""` 반환 → sel_key 초기화 → 클릭 패널 닫힘. 해결: `if _loc:` 가드 추가.
  - **마커 디자인**: 파랑 원형(`#1D4ED8`, size=16, opacity=0.95). choropleth 4색(빨강/주황/초록/회색)과 색상 대비 확보. hover: 의원명 + 종별 + 주소.

---

### 5. 현재 파일 구조

```
개원포화도/
├── .env                         # API 인증키 및 ADMIN_PASSWORD
├── config.py                    # 환경변수 로드 및 전역 설정
├── app.py                       # Streamlit 메인 대시보드 (UI/지도/로직 통합)
├── modules/
│   ├── population_api.py        # 인구 데이터 수집 (전국/시도/동 단위, 재시도 로직 포함)
│   ├── hospital_api.py          # 병의원 데이터 수집 (심평원 실시간 연동, 34개 과목)
│   └── data_merge.py            # 데이터 병합, 코드 표준화 및 지수 산출
├── scripts/                     # DB 마이그레이션 및 관리 스크립트 모음
│   ├── create_local_db.py       # 원본 엑셀 파일을 SQLite DB로 파싱 생성
│   ├── import_apt_price.py      # 아파트 실거래가 Excel → apt_price_bjd 테이블 적재
│   ├── migrate_to_supabase.py   # 구축된 로컬 DB를 Supabase로 Bulk Insert
│   └── update_db_from_api.py    # 공공 API 주기적 호출로 DB 최신화 (배치용)
├── data/
│   ├── saturation.db            # 생성된 중심 Local SQLite 데이터베이스 파일
│   └── geojson/
│       ├── national_dong.geojson  # 전국 행정동 경계 (원본)
│       └── seoul_dong.geojson     # 서울 전용 (백업용)
├── DB_data/
│   ├── 아파트 실거래가/           # 국토부 실거래가 Excel (아파트(매매)_실거래가_YYYYMM.xlsx)
│   └── ...                      # 기타 인구/병원 원본 엑셀
└── GUIDE/                       # API 가이드 문서 및 인증키 정보
```

---

### 6. 핵심 알고리즘: 계층적 코드 매핑 (Standardization)

분석의 정확도는 아래 3가지 코드 체계의 일치 여부에 달려 있습니다.
1. **인구 API**: 신표준 코드 (강원 51, 전북 52, 세종 36)
2. **HIRA API**: 심평원 독자 코드 (대구 23, 인천 22 등)
3. **GeoJSON**: 지도 경계 코드 (구코드 기반 42, 45, 41 등)

**[해결책]**: 모든 데이터는 수집 즉시 **인구 API 표준 코드**를 기준으로 `match_key`를 생성하도록 `_finalize_key` 함수가 실시간 번역을 수행함.

---

### 7. 진료과목 코드표 (HIRA API `dgsbjtCd` 기준)

> 출처: `GUIDE/OpenAPI활용가이드_건강보험심사평가원(의료기관별상세정보서비스).docx`

#### 의과
| 코드 | 과목명 | 코드 | 과목명 | 코드 | 과목명 |
|------|--------|------|--------|------|--------|
| 01 | 내과 | 02 | 신경과 | 03 | 정신건강의학과 |
| 04 | 외과 | 05 | 정형외과 | 06 | 신경외과 |
| 07 | 흉부외과 | 08 | 성형외과 | 09 | 마취통증의학과 |
| 10 | 산부인과 | 11 | 소아청소년과 | 12 | 안과 |
| 13 | 이비인후과 | 14 | 피부과 | 15 | 비뇨의학과 |
| 16 | 영상의학과 | 17 | 방사선종양학과 | 18 | 병리과 |
| 19 | 진단검사의학과 | 20 | 결핵과 | 21 | 재활의학과 |
| 22 | 핵의학과 | 23 | 가정의학과 | 24 | 응급의학과 |
| 25 | 직업환경의학과 | 26 | 예방의학과 | | |

#### 치과
| 코드 | 과목명 | 코드 | 과목명 | 코드 | 과목명 |
|------|--------|------|--------|------|--------|
| 49 | 치과 | 52 | 치과교정과 | 53 | 소아치과 |
| 54 | 치주과 | 55 | 치과보존과 | 61 | 통합치의학과 |

#### 한방
| 코드 | 과목명 | 코드 | 과목명 | 코드 | 과목명 |
|------|--------|------|--------|------|--------|
| 80 | 한방내과 | 81 | 한방부인과 | 82 | 한방소아과 |
| 83 | 한방안이비인후피부과 | 84 | 한방신경정신과 | 85 | 침구과 |
| 86 | 한방재활의학과 | 87 | 사상체질과 | | |

> ⚠️ **구 코드표와 혼동 금지**: `"18"=병리과`, `"21"=재활의학과`, `"22"=핵의학과`, `"23"=가정의학과`, `"24"=응급의학과`. 과거 코드에 오기입 있었음 (18→재활의학과로 잘못 매핑하던 버그 수정 완료).

---

### 8. 알려진 이슈 및 트러블슈팅

| 증상 | 원인 | 해결책 |
|------|------|--------|
| `Cannot set a DataFrame with multiple columns to the single column match_key` | geopandas sjoin 후 중복 컬럼 발생 | `loc[:, ~columns.duplicated()]`로 apply 전 중복 제거 |
| `KeyError: 'specialty_cd'` | h_frames 빈 경우 컬럼 없는 빈 DataFrame 생성 | `pd.DataFrame(columns=HOSPITAL_COLUMNS)` 폴백 |
| 의원수/전문의수 0개 | `clCd=31,21,11` 다중값 → HIRA API 0건 반환 | 단일값만 전달, 복수 시 파라미터 생략 |
| `RemoteDisconnected` | sido 레벨 연속 POST 50+ 건 → JUMIN 서버 강제 종료 | 3회 재시도 + 세션 재생성 + 구 간 0.8초 딜레이 |
| 병의원 API 오류 무시 | HTTP 200이어도 XML 내 오류코드 존재 | `_parse_response`에서 `resultCode != "00"` 감지 후 예외 발생 |
| 네이버 지도 검색 실패 | 전체 주소 또는 시도코드 노출 시 검색 불일치 | 검색 쿼리를 가장 정확도 높은 `병원명 + 시군구명(예: 광진구)` 조합으로 개선 |
| 개설일자 "정보 없음" 처리 | SQLite 이관 과정에서 `YYYYMMDD 00:00:00` 문자열 저장 | 공백 기준 `split()` 후 앞자리 날짜만 추출하도록 파싱 파이프라인 강화 |
| 버튼 글자색 미적용 | Streamlit이 버튼 내부 `<p>` 태그에 별도 색상 지정 | CSS 선택자에 `*` 자식 선택자 추가로 모든 하위 요소 강제 적용 |
| 지도·막대 색상 불일치 | 지도는 SI 연속값 기반, 막대는 등급 기반 색상 사용 | 지도를 `saturation_level` 기반 이산 4색으로 교체하여 동일 색 체계 통일 |
| sido 분석에서 수원시·고양시·용인시 등이 구 단위로 분리 표시 | `get_sgg_list`가 시·구 레벨을 모두 반환 → 각 구마다 별도 match_key 생성 | `existing_sgg_codes` 집합 빌드 후 `city_5 in sgg_codes` 체크로 구→시 통합; 인구 합산 처리 추가 |
| 부천시 sido 분석 시 의원 0개 표시 | 구폐지(2016) 후 population DB는 구 코드(41192 등) 유지, GeoJSON은 통합 코드(41190) 사용 → match_key 불일치 | `existing_sgg_codes` 체크 동일 로직으로 4119x → 41190 통합 처리 |
| sido dissolve 후 구 경계가 별도 폴리곤으로 남음 | `dissolve_key = gdf["adm_cd2"].str[:5]` 단순 슬라이싱이 구 코드(41111 등)를 그대로 사용 | `app.py`의 dissolve_key 계산을 `existing_sgg_codes` 기반 동적 판별로 교체 |
| `ValueError: Invalid property ... scattermapbox.Marker: 'line'` | `go.Scattermapbox` Marker는 `line` 속성을 지원하지 않음 (Scattermap과 다름) | `line=dict(width=..., color=...)` 제거 |
| Scattermapbox 마커가 지도에 표시 안 됨 | `symbol="marker"` (Maki 아이콘)은 Mapbox 공개 토큰 없이 carto-positron 스타일에서 렌더링 불가 → 트레이스 전체 무시됨 | `symbol` 파라미터 제거, 기본 circle 사용 |
| 의원 마커 클릭 시 행정동 선택 패널이 닫힘 | `selection.points[0].get("location", "")` → Scattermapbox 마커 클릭 시 `"location"` 키 없어 `""` 반환 → sel_key 초기화 | 클릭 핸들러에 `if _loc:` 가드 추가 (빈 문자열이면 sel_key 갱신 안 함) |
| 의원 핀이 포화 지역(빨강)에서 안 보임 | choropleth 4색 중 하나(빨강 `#DC2626`)와 동일한 색으로 마커 지정 | choropleth 4색(빨강/주황/초록/회색)과 대비되는 짙은 파랑(`#1D4ED8`)으로 변경 |

---

### 9. 향후 과제 (Next Steps)

로컬 DB 기반 아키텍처 개편 및 UI 고도화가 1차적으로 완료됨에 따라, 앞으로 시스템을 더 발전시키기 위해 다음 단계들을 계획할 수 있습니다.

#### ✅ 채택된 데이터 업데이트 방식: Excel 수동 재적재

> **API 자동화 방식은 채택하지 않습니다.**
>
> 검토 결과, 공공데이터 API 자동화(scripts/update_db_from_api.py)는 아래 이유로 현실적이지 않다고 판단되었습니다.
> - **병원 데이터**: HIRA API의 `sgguCd`(API 코드)와 DB에 저장된 Excel 기반 `sigungu_cd` 코드 체계가 불일치하여 업데이트 시 데이터 정합성 문제 발생
> - **인구 데이터**: 행안부 JUMIN API는 세션 인증·연결 끊김 등 안정성 문제로 자동화 난이도가 높으며, `update_db_from_api.py` 내에도 미구현 상태
> - **갱신 주기**: 두 데이터 모두 월 1회 갱신이므로 자동화의 실익이 크지 않음
>
> 대신 **월 1회 Excel 수동 재적재** 방식을 채택합니다.

**정기 업데이트 절차 (월 1회):**

```
1. 공공데이터포털에서 최신 Excel 파일 다운로드
   - 병원: 심평원 "전국 병의원 및 약국 현황" (1.병원정보서비스, 5.진료과목정보)
   - 인구: 행안부 "주민등록 인구 및 세대현황" (월별), "연령별 인구현황" (월별)

2. 다운로드한 파일을 DB_data/ 폴더에 덮어쓰기

3. DB 재구축 실행 (약 5~15분)
   python scripts/create_local_db.py

4. (Supabase 사용 시) 온라인 DB 동기화
   python scripts/migrate_to_supabase.py
```

---

| 단계 (Phase) | 과제명 | 세부 내용 및 목표 | 중요도 |
|--------------|--------|-------------------|--------|
| **Step 1** | **정기 Excel 재적재 환경 정비** | `DB_data/` 폴더 내 파일 명명 규칙을 `create_local_db.py`와 일치시키고, 업데이트 절차를 내부 문서화. `update_db_from_api.py`는 참고용으로 보관하되 실운용에서는 사용하지 않음. | 높음 |
| **Step 2** | **지도 폴리곤 최신화** | 2024~2025년 사이 신설되거나 통폐합된 행정동(예: 상일1동, 개포3동 등)의 최신 공간 데이터(GeoJSON)를 교체하여 지도상에 나타나지 않는 블랭크 지역 완벽 해소. | 중간 |
| **Step 3** | **온라인 DB (Supabase) 배포** | Streamlit 웹앱을 타인에게 배포하거나 호스팅(Streamlit Cloud 등) 환경으로 전환할 시기 도래 시 적용. `migrate_to_supabase.py`는 이미 완성됨. 단, 웹앱 코드(`population_api.py`, `hospital_api.py`)의 DB 연결부를 SQLite → Supabase 클라이언트로 교체해야 함. 변경 파일 목록은 아래 참조. | 선택 |
| **Step 4** | **AI 입지 분석 리포트 도입** | Gemini API 등 LLM을 연동. 사용자가 분석 실행 시 "포화도는 1.5로 여유로우며, 배후 11만 세대 대비 내과 개원이 매우 유리합니다" 와 같은 텍스트 기반 인사이트 요약 패널 추가. | 장기 |
| ~~**Step 5**~~ | ~~**아파트 실거래가(소득 지표) 통합**~~ | ✅ **완료 (21단계)** — `scripts/import_apt_price.py` + `enrich_with_apt_price()` + `_make_scatter_chart()` 구현. 14개월치(607,586건) 파싱, 3,452개 법정동 평당가 저장, 소득×포화도 산점도 UI 연동. | 완료 |

---

### 10. 아파트 실거래가 + 개비공 소득지수 데이터 파이프라인 ✅ 구현 완료

> **행정동 단위 소득 실데이터 부재에 따른 대안 구축 방안 — 21단계(평당가 파이프라인) + 22단계(소득지수 가공)에서 구현 완료**

- **배경**: 전국의 행정동 단위 평균 소득 데이터는 무료 공공데이터로 전체 공개되지 않음.
- **해결책**: 국토교통부 아파트 매매 실거래가를 수집하여 **법정동별 아파트 평당 평균 거래가**를 소득 대리 지표로 활용.

#### 데이터 현황

| 항목 | 내용 |
|------|------|
| 데이터 기간 | 2025.01 ~ 2026.02 (14개월) |
| 총 거래 건수 | 607,586건 |
| 저장 법정동 수 | 3,452개 (`apt_price_bjd` 테이블) |
| 매핑 성공률 | 100.0% |
| 행정동 커버리지 | 3,452개 법정동 → 2,663개 행정동 연결 |

#### 파이프라인 구조

```
[DB_data/아파트 실거래가/*.xlsx]  (아파트(매매)_실거래가_YYYYMM.xlsx)
      ↓ python scripts/import_apt_price.py
[전처리]
 - skiprows=12, header=0 (헤더는 13번째 행)
 - 거래금액(만원): "120,000" → 쉼표 제거 후 float
 - 평당가 = 거래금액(만원) × 3.3058 / 전용면적(㎡)
 - 시군구 컬럼 파싱: parts[0]=시도, parts[-1]=법정동명, 중간 읍면 토큰 제거
      ↓ 3단계 Fallback join (region_code_mapping)
 ① (sido_nm, sigungu_nm, bjd_nm) 완전 일치
 ② (sido_nm, sigungu 마지막 단어, bjd_nm)
 ③ (sido_nm, bjd_nm) — sigungu 무시 폴백
      ↓ bjd_cd 획득 (매핑 성공률 100%)
[법정동별 집계] GROUP BY bjd_cd → 평균/중위 평당가, 거래건수
      ↓
[apt_price_bjd 테이블] bjd_cd | avg_price | med_price | trade_count | base_ym_from | base_ym_to
      ↓ data_merge.py: enrich_with_apt_price()
[region_code_mapping join] bjd_cd → hjd_cd (거래량 가중 평균 SQL)
      ↓ match_key 레벨별 집계 (dong=10자리 / sido=5자리 / national=2자리)
[si_df에 avg_price_per_pyeong 컬럼 추가]
      ↓ app.py
[지도 hover / 클릭 지표 6번째 / 소득×포화도 산점도]
```

#### `apt_price_bjd` 테이블 스키마

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `bjd_cd` | TEXT | 법정동 코드 10자리 (INDEX) |
| `avg_price_per_pyeong` | INTEGER | 평균 평당가 (만원/평) |
| `med_price_per_pyeong` | INTEGER | 중위수 평당가 (만원/평, 이상치에 강건) |
| `trade_count` | INTEGER | 거래건수 (신뢰도 지표) |
| `base_ym_from` | TEXT | 데이터 시작 년월 (예: "202501") |
| `base_ym_to` | TEXT | 데이터 종료 년월 (예: "202602") |

#### 개비공 소득지수 계산 흐름 (22단계 추가)

```
[apt_price_bjd] avg_price_per_pyeong (법정동 평균 평당가)
      + [population_age] 30~59세 인구비율
      ↓ calc_income_index() — data_merge.py
  log2(평당가) → 전국 min-max → price_score (0~100)
  30~59세비율  → 전국 min-max → active_score (0~100)
  composite = price_score×0.7 + active_score×0.3
  전국 quintile → S(80%↑) / A(60%) / B(40%) / C(20%) / D(0%)
      ↓ si_df에 income_score, income_grade 컬럼 추가
[app.py]
  지도 hover: "개비공 소득지수: A"
  클릭 패널 6번째 지표: 등급 컬러 배지
  산점도 X축: income_score (0~100점)
```

#### 산점도 읽는 법 (개비공 소득지수 × 포화도 입지 분석)

```
y축(포화도 ↑) × x축(개비공 소득점수 ↑, 0-100)

[기회 지역]     │   [최적 입지 ★]
 저소득 · 여유  │   고소득 · 여유
────────────────┼────────────────  ← SI = 1.0 (전국 평균)
   [불리]       │     [주의]
 저소득 · 포화  │   고소득 · 포화
                ↑ 소득점수 50점
```

#### 정기 업데이트 방법 (연 1회 권장)

```bash
# 1. DB_data/아파트 실거래가/ 폴더에 최신 Excel 파일 추가 (국토부 실거래가 공개시스템)
# 2. 적재 스크립트 실행 (기존 apt_price_bjd 테이블 교체)
python scripts/import_apt_price.py
```

---

### 11. 온라인 DB 이관 계획 (Supabase)

> **현재 미진행 — 향후 외부 배포(Streamlit Cloud 등) 시점에 실행**

#### 접근 방식: psycopg2 직접 연결
Supabase는 PostgreSQL 기반이므로 `psycopg2`로 직접 연결한다.
현재 코드의 `pd.read_sql_query(query, conn)` 패턴을 그대로 유지할 수 있어 코드 변경이 최소화된다.
`.env`에 `SUPABASE_DB_URL` 설정 여부로 SQLite(로컬) ↔ Supabase(온라인)를 자동 전환한다.

#### 수정 필요 파일

| 파일 | 변경 내용 | 난이도 |
|------|-----------|--------|
| `.env` | `SUPABASE_DB_URL=postgresql://postgres:[password]@db.[ref].supabase.co:5432/postgres` 추가 | 매우 쉬움 |
| `config.py` | `SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "")` 1줄 추가 | 매우 쉬움 |
| `modules/population_api.py` | `_get_conn()` 수정: SUPABASE_DB_URL 있으면 `psycopg2.connect()`, 없으면 기존 `sqlite3.connect()` | 쉬움 |
| `modules/hospital_api.py` | 동일 + `_ph()` 헬퍼 추가(SQLite=`?`, PostgreSQL=`%s`) + 쿼리 내 `?` → `{_ph()}` 교체 | 중간 |
| `scripts/migrate_to_supabase.py` | REST API 방식 → psycopg2 방식으로 재작성. ① 테이블 DDL 생성 → ② SQLite 읽기 → ③ TRUNCATE → ④ bulk INSERT (`execute_values`, 500건 단위) | 중간 |
| `requirements.txt` | `psycopg2-binary>=2.9.0` 추가 | 매우 쉬움 |

#### 변경 불필요 파일

| 파일 | 이유 |
|------|------|
| `app.py` | 모듈 인터페이스 유지, 수정 불필요 |
| `modules/data_merge.py` | 동일 |
| `scripts/create_local_db.py` | 로컬 DB 구축용, Supabase와 무관하게 계속 사용 |
| `data/geojson/` | 변경 없음 |

#### SQL 호환성 메모

| 항목 | SQLite | PostgreSQL |
|------|--------|-----------|
| 플레이스홀더 | `?` | `%s` |
| LIKE 패턴 | 동일 | 동일 (단, 인덱스는 `text_pattern_ops` 필요) |
| f-string 쿼리 | 동일 | 동일 |
| `pd.read_sql_query` | 동일 | 동일 |

#### 실행 순서 (이관 시점 도래 시)
```
1. pip install psycopg2-binary
2. .env에 SUPABASE_DB_URL 추가
   (Supabase 대시보드 → Settings → Database → Connection string → URI)
3. python scripts/migrate_to_supabase.py   # 로컬 SQLite → Supabase 복사 (5~20분)
4. streamlit run app.py                    # 자동으로 Supabase 사용
```

---
