# -*- coding: utf-8 -*-
"""수집 대상 API 설정.

지역 코드 체계가 둘이다.
- 국문 관광정보(KorService2): TourAPI areaCode (1~39)
- 데이터랩 계열(방문자수/수요/강도/연관): 행정 areaCd/signguCd (11~52, regions.py)
"""

MOBILE_OS = "ETC"
MOBILE_APP = "bdp"
RESP_TYPE = "json"

# 수도권(서울1·인천2·경기31) 제외 TourAPI areaCode
AREA_CODES_LOCAL = [3, 4, 5, 6, 7, 8, 32, 33, 34, 35, 36, 37, 38, 39]
# 12관광지 14문화시설 15축제 25여행코스 28레포츠 32숙박 38쇼핑 39음식점
CONTENT_TYPE_IDS = [12, 14, 15, 25, 28, 32, 38, 39]

# A. 국문 관광정보 (관광지 좌표 마스터)
KOR = {
    "base_url": "https://apis.data.go.kr/B551011/KorService2",
    "endpoint": "areaBasedList2",
    "common_params": {
        "MobileOS": MOBILE_OS, "MobileApp": MOBILE_APP, "_type": RESP_TYPE,
        "arrange": "C",
    },
}

# C. 방문자수 (일자별, startYmd/endYmd)
VISITOR = {
    "base_url": "https://apis.data.go.kr/B551011/DataLabService",
    "endpoints": ["metcoRegnVisitrDDList", "locgoRegnVisitrDDList"],
    "common_params": {
        "MobileOS": MOBILE_OS, "MobileApp": MOBILE_APP, "_type": RESP_TYPE,
        "startYmd": "20250301", "endYmd": "20250531",
    },
}

# D. 관광 자원 수요 (areaCd + baseYm)
RES_DEMAND = {
    "base_url": "https://apis.data.go.kr/B551011/AreaTarResDemService",
    "endpoints": ["areaTarSvcDemList", "areaCulResDemList"],
    "common_params": {"MobileOS": MOBILE_OS, "MobileApp": MOBILE_APP, "_type": RESP_TYPE},
}

# E. 관광 수요 강도 (areaCd + baseYm)
DEM_INTENSITY = {
    "base_url": "https://apis.data.go.kr/B551011/AreaTarDemDsService",
    "endpoints": ["areaTarSjrnDsList", "areaTarExpDsList"],
    "common_params": {"MobileOS": MOBILE_OS, "MobileApp": MOBILE_APP, "_type": RESP_TYPE},
}

# B. 연관 관광지 (areaCd + signguCd + baseYm)
RELATED = {
    "base_url": "https://apis.data.go.kr/B551011/TarRlteTarService1",
    "endpoint": "areaBasedList1",
    "common_params": {"MobileOS": MOBILE_OS, "MobileApp": MOBILE_APP, "_type": RESP_TYPE},
}

# 연관 관광지 데이터 제공 범위 2024.05~2025.04 중 최신월
RELATED_BASE_YM = "202504"
# 자원수요/수요강도: 데이터가 있는 월을 최신→과거로 탐색
DEMAND_BASE_YMS = [
    "202504", "202503", "202502", "202501", "202412", "202411", "202410",
    "202409", "202408", "202407", "202406", "202405", "202404", "202403",
    "202402", "202401", "202312", "202311", "202310", "202309",
]
DEMAND_MONTHS_PER_AREA = 6
