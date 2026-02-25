"""
전국 행정동 경계 GeoJSON 다운로드 스크립트

실행 방법 (프로젝트 루트에서):
  python scripts/download_national_geojson.py

출력 파일:
  data/geojson/national_dong.geojson

속성 정규화:
  - adm_cd2 : 행정동 10자리 코드 (예: 1168051000)
  - adm_nm  : 행정동 전체 명칭 (예: 서울특별시 강남구 역삼1동)
"""

import json
import sys
from pathlib import Path

import requests

SOURCES = [
    # vuski/admdongkor — 전국 행정동 경계 (2023-04 기준)
    "https://raw.githubusercontent.com/vuski/admdongkor/master/ver20230401/HangJeongDong_ver20230401.geojson",
    # 백업 소스
    "https://raw.githubusercontent.com/raqoon886/Local_HangJeongDong/master/HangJeongDong_ver20230401.geojson",
]

OUT_PATH = Path(__file__).parent.parent / "data" / "geojson" / "national_dong.geojson"


def _normalize_properties(feature: dict) -> dict:
    """
    다양한 소스의 GeoJSON 속성을 adm_cd2 / adm_nm 으로 통일.
    """
    props = feature.get("properties", {})

    # ── adm_cd2 (10자리 행정동 코드) 확보 ──
    adm_cd2 = (
        props.get("adm_cd2")
        or props.get("ADM_CD2")
        or props.get("ADM_DR_CD")
        or props.get("adm_cd")
        or props.get("ADM_CD")
    )
    if adm_cd2 is None:
        return feature

    adm_cd2 = str(adm_cd2).strip()

    # 8자리 → 10자리 패딩 (일부 소스는 trailing 00 생략)
    if len(adm_cd2) == 8:
        adm_cd2 = adm_cd2 + "00"

    # ── adm_nm (행정동 전체 명칭) 확보 ──
    adm_nm = (
        props.get("adm_nm")
        or props.get("ADM_NM")
        or props.get("ADM_DR_NM")
        or props.get("EMD_NM")
        or props.get("DONG_NM")
        or ""
    )

    feature["properties"] = {**props, "adm_cd2": adm_cd2, "adm_nm": str(adm_nm).strip()}
    return feature


def download(url: str) -> dict | None:
    print(f"  → 다운로드 시도: {url}")
    try:
        r = requests.get(url, timeout=120, stream=True)
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        chunks = []
        downloaded = 0
        for chunk in r.iter_content(chunk_size=1024 * 512):
            chunks.append(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded / total * 100
                print(f"\r     {pct:.1f}% ({downloaded // 1024 // 1024} MB)", end="", flush=True)
        print()
        return json.loads(b"".join(chunks))
    except Exception as e:
        print(f"  ✗ 실패: {e}")
        return None


def main():
    print("=" * 60)
    print("전국 행정동 GeoJSON 다운로드")
    print("=" * 60)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    gj = None
    for url in SOURCES:
        gj = download(url)
        if gj:
            break

    if gj is None:
        print("\n✗ 모든 소스 다운로드 실패.")
        print("  아래 URL에서 수동으로 다운로드 후")
        print(f"  {OUT_PATH} 에 저장하세요:")
        for url in SOURCES:
            print(f"  {url}")
        sys.exit(1)

    # 속성 정규화
    print("  속성 정규화 중...")
    features = [_normalize_properties(f) for f in gj.get("features", [])]
    gj["features"] = features

    # 저장
    print(f"  저장 중: {OUT_PATH}")
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(gj, f, ensure_ascii=False)

    print(f"\n[OK] 완료: {len(features)}개 행정동 경계 저장")

    # 샘플 속성 출력
    sample = next(
        (f["properties"] for f in features if f.get("properties", {}).get("adm_cd2")),
        None,
    )
    if sample:
        print(f"  샘플 속성: adm_cd2={sample.get('adm_cd2')}, adm_nm={sample.get('adm_nm')}")


if __name__ == "__main__":
    main()
