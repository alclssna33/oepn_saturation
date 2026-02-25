"""
환경변수 로드 및 프로젝트 공통 설정
"""
import os
from dotenv import load_dotenv

load_dotenv()

# API 인증키
PUBLIC_DATA_API_KEY = os.getenv("PUBLIC_DATA_API_KEY", "")
JUSO_API_KEY = os.getenv("JUSO_API_KEY", "")

# 관리자 설정
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin1234")

# MVP 기본 지역
DEFAULT_SIDO = os.getenv("DEFAULT_SIDO", "서울특별시")
DEFAULT_SGG = os.getenv("DEFAULT_SGG", "강남구")

# 건강보험심사평가원 API 엔드포인트
HIRA_BASE_URL = "http://apis.data.go.kr/B551182"
HIRA_HOSPITAL_LIST_URL = f"{HIRA_BASE_URL}/hospInfoServicev2/getHospBasisList"
HIRA_HOSPITAL_DETAIL_URL = f"{HIRA_BASE_URL}/MdlInfoService/getMdlInfo"

# 행정안전부 인구 API 엔드포인트
POPULATION_BASE_URL = "https://jumin.mois.go.kr/ageStatMonth.do"

# 주소기반산업지원서비스 (좌표 변환) 엔드포인트
JUSO_API_URL = "https://business.juso.go.kr/addrlink/addrLinkApi.do"

# 주요 진료과목 코드 (건강보험심사평가원 기준)
SPECIALTY_CODES = {
    "내과": "01",
    "신경과": "02",
    "정신건강의학과": "03",
    "외과": "04",
    "정형외과": "05",
    "신경외과": "06",
    "흉부외과": "07",
    "성형외과": "08",
    "마취통증의학과": "09",
    "산부인과": "10",
    "소아청소년과": "11",
    "안과": "12",
    "이비인후과": "13",
    "피부과": "14",
    "비뇨의학과": "15",
    "영상의학과": "16",
    "방사선종양학과": "17",
    "병리과": "18",
    "진단검사의학과": "19",
    "결핵과": "20",
    "재활의학과": "21",
    "가정의학과": "22",
    "응급의학과": "23",
    "핵의학과": "24",
    "직업환경의학과": "25",
}


def validate_keys():
    """API 키 설정 상태를 확인하고 출력합니다."""
    print("=== API 키 설정 상태 ===")
    print(f"공공데이터포털 키: {'설정됨' if PUBLIC_DATA_API_KEY else '미설정'}")
    print(f"주소 API 키: {'설정됨' if JUSO_API_KEY else '미설정 (추후 발급 예정)'}")
    print(f"기본 지역: {DEFAULT_SIDO} {DEFAULT_SGG}")
    print("========================")


if __name__ == "__main__":
    validate_keys()
