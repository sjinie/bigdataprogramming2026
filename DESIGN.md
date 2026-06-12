# 빅데이터 프로그래밍 기말 프로젝트 설계서
## 지방 관광지 분석 및 이동동선 연계 시스템

**작성일:** 2026-06-08
**과목:** 빅데이터 프로그래밍 (명지대학교 인공지능전공)
**실행 환경:** GCP HDP Sandbox (Hadoop/HDFS, Hive, Spark, Sqoop 등)

> 이 문서는 **구현 이전 설계 단계** 산출물입니다. 확정되지 않았거나 실제 데이터 명세 확인이 필요한 부분은 본문에 `⚠️ 확인 필요`로 표시했습니다. 검토 후 합의되면 이 설계서를 기준으로 구현(스크립트·쿼리·시각화·README·보고서)을 진행합니다.

---

## 1. 프로젝트 개요

### 1.1 목표 (과제 요구서 1번 대응)
- 강의에서 다룬 Hadoop 프레임워크(HDFS, MapReduce, Hive, Spark, Sqoop)를 사용하여 빅데이터 분석 시스템을 구현한다.
- **데이터 수집 → 저장 → 처리/분석 → 결과 제시**의 end-to-end 파이프라인을 구축한다.
- 모든 산출물을 GitHub repository 한 개에 정리하여 공개한다.

### 1.2 문제 정의 (사용자 정의)
수도권 집중과 특정 관광지 수요 편중은 지방 소멸 위기·지역 격차로 이어진다. 지방의 우수 관광자원은 충분히 알려지지 않았고, 관광객은 넓게 흩어진 관광지를 **무엇을·어떤 순서로** 방문할지 정보를 얻기 어렵다. 관광지 간 연계가 약해 단일 관광지 방문에 그치는 경향이 있다.

### 1.3 핵심 분석 질문
1. **인기도:** 수도권을 제외한 지방(광역시·도 단위)에서 어떤 지역·관광지가 인기 있는가?
2. **연계성:** 인기 관광지 인근에서 함께 방문하기 좋은 연계 관광지는 어디인가?
3. **동선:** 지리적 위치와 방문 패턴을 고려한 추천 이동동선을 어떻게 구성할 수 있는가?

---

## 2. 강의 내용과의 기술 매핑

과제 2.4는 "강의 실습 환경(GCP HDP Sandbox)에서 실행 가능"을 요구합니다. 아래는 파이프라인 단계별로 강의에서 다룬 기술을 어떻게 배치하는지에 대한 설계입니다.

| 파이프라인 단계 | 사용 기술 | 강의 자료 근거 |
|---|---|---|
| 수집 자동화 | Python(requests) + Bash 스크립트 | 12_2_Sqoop Flume.pdf (수집), 09_GCP Setup |
| 적재(저장) | **HDFS** (`hdfs dfs -put`) | 07_2_MapReduce HDFS.pdf, 10_HDFS MapReduce Hands-on |
| (선택) RDB→HDFS 연동 | **Sqoop** | 12_2_Sqoop Flume.pdf |
| 전처리 | **Spark DataFrame**, **Hive** | 11_2/12_1_Spark, 11_1_Pig & Hive.pdf |
| 분석 | **Spark SQL**, **HiveQL** | 11_1_Pig & Hive.pdf, 12_1_Spark_2.pdf |
| 저장 포맷 변환 | **Parquet** (컬럼 포맷) | 11/12 Spark 자료 |
| 시각화·결과 | Python(matplotlib/folium) | — |

> 설계 의도: 단순 SELECT가 아닌 `GROUP BY` / `JOIN` / 통계/지리연산을 Spark SQL·HiveQL로 수행하여 과제 2.2의 "의미 있는 분석" 요건을 충족합니다. MapReduce는 원시 JSON 카운팅 등 보조 작업에 1회 이상 명시적으로 사용하여 "MapReduce 활용" 요건을 충족합니다(8장 참고).

---

## 3. 데이터 소스 명세 (과제 2.2 대응)

### 3.1 확보 데이터 목록

| # | 데이터 | 제공 형태 | 주요 역할 | 대응 질문 | 신청 상태 |
|---|---|---|---|---|---|
| A | 한국관광공사 **국문 관광정보 서비스_GW** (TourAPI, ID **15101578**) | OpenAPI(JSON+XML) | 관광지 마스터: 명칭·주소·**위경도(mapx/mapy)**·분류코드·개요·이미지 | Q1,Q2,Q3 | ✅ 신청 완료 |
| B | 한국관광공사 **관광지별 연관 관광지 정보** (ID **15128560**) | OpenAPI(JSON+XML) | **Tmap 내비 기반** 중심관광지별 연관 관광지(전체/관광지/음식/숙박 유형별 최대 50위) | Q2,Q3 | ✅ 신청 완료 |
| C | **빅데이터_지역별 방문자수_GW** (ID **15101972**) | OpenAPI(JSON+XML) | 광역·기초 지자체 **일별 방문자수**(KT 내국인+SKT 외국인 이동통신 기반) | Q1 | ✅ 신청 완료 |
| D | **지역별 관광 자원 수요** (ID **15152138**) | OpenAPI(JSON+XML) | 지역별 관광 서비스 수요·문화 자원 수요 지수 | Q1 | ✅ 신청 완료 |
| E | **지역별 관광 수요 강도** (ID **15151868**) | OpenAPI(JSON+XML) | 지역별 관광 체류 강도·소비 강도 지수 | Q1 | ✅ 신청 완료 |

**확정된 OpenAPI 명세 (브라우저로 data.go.kr 직접 확인, 2026-06-08):**

| 데이터 | Base URL | 오퍼레이션(GET) | 제공 단위 |
|---|---|---|---|
| C (15101972) | `apis.data.go.kr/B551011/DataLabService` | `/metcoRegnVisitrDDList`(광역 일별 방문자수), `/locgoRegnVisitrDDList`(기초 일별 방문자수) | 광역/기초, **일자별 순방문자수** |
| D (15152138) | `apis.data.go.kr/B551011/AreaTarResDemService` | `/areaTarSvcDemList`(관광 서비스 수요), `/areaCulResDemList`(문화 자원 수요) | 지역(지자체) |
| E (15151868) | `apis.data.go.kr/B551011/AreaTarDemDsService` | `/areaTarSjrnDsList`(관광 체류 강도), `/areaTarExpDsList`(관광 소비 강도) | 지역(지자체) |

- 공통: REST, JSON+XML, 무료, **개발계정 일 1,000건 트래픽 제한**, 실시간 갱신, 출처=한국관광 데이터랩(datalab.visitkorea.or.kr). 각 데이터 상세페이지의 `참고문서`(TourAPI_Guide zip)에 요청 파라미터 전체 명세가 포함됨.
- C는 **신용카드·이동통신·내비게이션이 아닌 이동통신 기반**이며 **일자별** 제공 → 날짜 범위 수집으로 시계열·누적용량 확보에 유리.
- D·E는 한국관광 데이터랩 "관광수요지수" 구성지표(신용카드·이동통신·내비게이션 빅데이터 기반).

> ⚠️ **확인 필요(요청 파라미터):** 각 오퍼레이션의 정확한 요청 파라미터(예: `MobileOS`, `MobileApp`, `startYmd/endYmd`, `signguCode`, `areaCode`, `numOfRows`, `pageNo`, `_type`)와 응답 필드명은 `참고문서` zip(또는 상세기능 펼침)에서 최종 확인합니다. 현재 일반 데이터랩 API 관례를 따른다고 가정하며, 구현 시 실제 샘플 호출로 검증합니다.
> **설계상 함의(확정):** C·D·E는 모두 **지역(광역/기초) 집계 지표**로 개별 관광지 **좌표(위경도)가 없습니다.** 따라서 연계·동선(Q2·Q3)에는 좌표를 제공하는 TourAPI(A)와 연관 관광지 API(B)를 사용합니다 — **둘 다 신청 완료.**

**A·B 확정 명세 (브라우저로 data.go.kr 직접 확인, 2026-06-08):**

| 데이터 | Base URL | 주요 오퍼레이션(GET) | 비고 |
|---|---|---|---|
| A (15101578) | `apis.data.go.kr/B551011/**KorService2**` | `/areaBasedList2`(지역기반), `/locationBasedList2`(위치기반), `/detailCommon2`(공통·좌표), `/detailIntro2`·`/detailInfo2`·`/detailImage2`(상세), `/areaCode2`·`/categoryCode2`·`/ldongCode2`·`/lclsSystmCode2`(코드) | 전국 약 26만 건. **오퍼레이션 접미사 "2"(KorService2 신버전)** 사용. `mapx`(경도)·`mapy`(위도) 제공 |
| B (15128560) | `apis.data.go.kr/B551011/**TarRlteTarService1**` | `/areaBasedList1`(지역기반 연관 관광지 목록), `/searchKeyword1`(키워드 검색) | **Tmap 내비** 기반(목적지 조회 + 100m·1분 이상 이동 충족분 집계). 중심관광지별 전체/관광지/음식/숙박 유형 **각 최대 50위**. 기간 **2024-05 ~ 2025-04** |

> ⚠️ **확인 필요(요청 파라미터):** A·B 각 오퍼레이션의 정확한 요청 파라미터/응답 필드는 상세페이지 `참고문서` zip(A: 개방데이터_활용매뉴얼(국문).zip, B: TourAPI_Guide_(연관관광지)v4.1.zip)에서 확정. 공통 파라미터는 `serviceKey`, `MobileOS`, `MobileApp`, `numOfRows`, `pageNo`, `_type`(json) 관례를 따를 것으로 보이며 구현 시 샘플 호출로 검증합니다.
> **B의 강점:** "사람들이 A 다음에 어디로 이동했나"를 Tmap 데이터로 직접 제공 → Q2(연계 관광지)·Q3(동선)의 **핵심 근거 데이터**. 좌표 기반 거리계산(A)과 교차검증하면 분석의 본인 기여(§7)가 강화됩니다.

### 3.2 데이터 역할 분담 설계 (근거)
- **인기도(Q1):** C(일별 방문자수) + D(관광 자원 수요) + E(관광 수요 강도) 집계 지표.
- **연계(Q2):** B(연관 관광지, Tmap 기반)가 직접 제공 + A 좌표로 근접성 교차검증.
- **동선(Q3):** B의 중심→연관 연결성 + A의 좌표를 결합해 추천 동선 구성.
- 모든 데이터는 **기초/광역 지자체 코드** 또는 **관광지명**으로 연결 가능 → 조인 키 설계가 핵심(§5.3).

### 3.3 100MB 누적 확보 전략 (과제 2.2 필수요건)
TourAPI 국문 관광정보(A)는 전국 약 26만 건 규모입니다([출처](https://api.visitkorea.or.kr/)). 수집 설계:

1. **`/areaBasedList2`**(지역기반관광정보)로 수도권 제외 14개 시·도 × 8개 콘텐츠타입(관광지12/문화시설14/축제15/여행코스25/레포츠28/숙박32/쇼핑38/음식점39) 목록 전수 수집.
2. 각 콘텐츠에 대해 **`/detailCommon2` / `/detailIntro2` / `/detailInfo2` / `/detailImage2`** 상세 호출로 JSON 누적.
3. C·D·E 집계 데이터 + B(연관 관광지) 원본 동시 적재.

추정: 지방 POI 약 3~5만 건 × 상세 4종 × 평균 2~4KB ≈ **수백 MB 규모** → 100MB 요건 여유 있게 충족.

**추가 레버 — C(방문자수)의 일별 시계열:** C(15101972)는 광역/기초 지자체 **일자별** 순방문자수를 제공하므로, 예컨대 최근 1~2년치를 기초 지자체(약 230여 개) × 일자로 수집하면 그 자체로 수십~수백만 행이 누적됩니다. TourAPI 없이도 100MB 달성이 가능하며, 시계열(월별 추이·계절성) 분석 질문도 자연스럽게 추가됩니다.
> 검증: 수집 스크립트에 `du -sh data/raw/` 로깅을 넣어 100MB 도달을 자동 확인하도록 설계합니다.
> 트래픽: 개발계정은 API당 **일 1,000건 제한**이므로, C의 일별 전수 수집은 `numOfRows`를 크게 잡아 호출 수를 줄이고, 필요 시 일자별 반복 수집(여러 날에 나눠 누적)으로 운용합니다.
> 수도권 제외 정책상 areaCode 1(서울)·2(인천)·31(경기)을 제외합니다.

### 3.4 수집 재실행성 (과제 2.2 요건)
- API 인증키는 코드에 하드코딩하지 않고 **환경변수**(`TOUR_API_KEY`)로 주입.
- 수집 스크립트(`collect.py`)는 `--area`, `--content-type`, `--from-page` 인자를 받아 **재실행/이어받기** 가능하도록 설계.
- 페이지네이션·요청제한(rate limit)·재시도(backoff) 처리 포함.

---

## 4. 시스템 아키텍처

```
[공공데이터 API/파일]
   │  (1) Python/Bash 수집 스크립트 (재실행 가능, 환경변수 키)
   ▼
[로컬 staging: data/raw/*.json, *.csv]
   │  (2) hdfs dfs -put  (Sqoop은 RDB 경유 시 옵션)
   ▼
[HDFS: /user/tour/raw/...]   ← 원시 보관 (JSON/CSV)
   │  (3) Spark/Hive 전처리: 결측치·타입변환·좌표정제·조인·집계
   ▼
[HDFS: /user/tour/curated/...parquet]  ← 분석용 정제 테이블(Parquet)
   │  (4) Spark SQL / HiveQL 분석 (GROUP BY/JOIN/통계/지리연산)
   ▼
[분석 결과: results/*.csv]
   │  (5) Python 시각화 (matplotlib / folium 지도)
   ▼
[보고서·발표자료·README]
```

> MapReduce는 (3) 보조 단계에서 원시 JSON의 콘텐츠타입별 레코드 카운트 잡으로 1회 명시 사용하여 강의 핵심기술 사용 근거를 남깁니다.

---

## 5. 단계별 상세 설계

### 5.1 수집 (Collect)
- `src/collect/collect_tourapi.py` — areaBasedList → 상세조회 파이프라인.
- `src/collect/collect_visitor.py` — C·D·E 집계 데이터(API/파일) 다운로드.
- `src/collect/run_collect.sh` — 전체 수집 오케스트레이션 + 용량 로깅.
- 출력: `data/raw/poi/area={code}/type={id}/*.json`, `data/raw/visitor/*.csv`.

### 5.2 저장 (HDFS 적재)
- `scripts/hdfs_put.sh`:
  ```bash
  hadoop fs -mkdir -p /user/maria_dev/tour/raw/poi /user/maria_dev/tour/raw/visitor
  hadoop fs -put -f data/raw/poi/*     /user/maria_dev/tour/raw/poi/
  hadoop fs -put -f data/raw/visitor/* /user/maria_dev/tour/raw/visitor/
  ```
- 원시는 JSON/CSV 그대로 보관 → 재현성 확보. 정제본은 Parquet로 별도 디렉터리.

### 5.3 전처리 (Spark/Hive)
설계 포인트(과제 2.2 전처리 요건: 결측치·타입변환·필터링·조인·집계 모두 포함):
- **파싱·평탄화:** 중첩 JSON → 평탄 DataFrame.
- **타입 변환:** `mapx/mapy`(문자열) → double(경도/위도), 코드값 → 카테고리.
- **결측치:** 좌표 결측 행 제거 또는 분리, 주소 결측 보정.
- **필터링:** 수도권(areaCode 1,2,31) 제외.
- **조인:** POI(A) ↔ 지역 집계(C·D·E)를 `areaCode/sigunguCode` 기준 조인하여 "지역 인기도 + 개별 POI" 통합 테이블 생성.
- **표준화:** 시도/시군구 코드 ↔ 명칭 매핑 테이블(areaCode2) 구축.
- 산출: `curated.poi`, `curated.region_demand`, `curated.poi_related` (Parquet/Hive 외부테이블).

### 5.4 분석 (Spark SQL / HiveQL) — 6장 참조

### 5.5 시각화
- **막대/추이:** 지역별 방문자수·수요강도 순위 (matplotlib).
- **지도:** folium으로 인기 POI 마커 + 연계 관광지 + 추천 동선 polyline 표시 → HTML 지도 산출.
- 결과 이미지는 `results/figures/`에 저장하여 보고서·README에 삽입.

---

## 6. 분석 질문 상세 설계 (과제 2.2: 최소 3개)

### Q1. 지방 인기 지역·관광지 (집계·순위)
- **방법:** 지역별 방문자수/수요강도(C·D·E)를 시도·시군구로 `GROUP BY` 후 순위화. POI 밀도(A의 콘텐츠 수)와 결합.
- **예시 쿼리(Spark SQL):**
  ```sql
  SELECT sido, sigungu, SUM(visitor_cnt) AS total_visitors,
         AVG(demand_intensity) AS avg_intensity
  FROM curated_region_demand
  WHERE sido NOT IN ('서울','인천','경기')
  GROUP BY sido, sigungu
  ORDER BY total_visitors DESC;
  ```

### Q2. 연계 관광지 (조인·근접성)
- **방법 A (API 기반):** 연관 관광지 API(B)로 인기 POI별 연관 목록 직접 활용.
- **방법 B (좌표 기반, 본인 기여):** 인기 POI 반경 R km 내 다른 콘텐츠타입 POI를 **Haversine 거리**로 추출 → "함께 방문 좋은 곳" 후보. 두 방법을 교차검증.
- **예시:** 인기 관광지(type=12) 기준 반경 5km 내 음식점(39)·숙박(32)·문화시설(14) 카운트 및 거리순 정렬.

### Q3. 추천 이동동선 (지리·패턴)
- **방법:** Q2 후보들을 거리행렬로 묶어 **근접 클러스터링** 후, 시작점→인근 POI를 거리 기반 greedy/최근접 순서로 연결한 동선 생성(소규모 TSP 근사). 지도에 polyline 시각화.
- **본인 기여 포인트:** 단순 데이터 재현이 아니라 "인기도 가중 + 거리 기반 동선 구성"이라는 분석 관점을 직접 설계(과제 2.5 충족).

> 여력 시 추가 질문: "콘텐츠타입별 지역 편중도(시도별 음식점 vs 관광지 비율)", "수요강도와 POI 밀도의 상관관계(상관계수)" 등으로 통계 분석을 보강.

---

## 7. 본인 기여 포인트 (과제 2.5 — 단순 재구현 금지)
1. **데이터 관점:** 집계형 수요데이터(C·D·E)와 좌표형 POI데이터(A·B)를 **조인**하여 "지역 인기도 × 개별 관광지"를 결합한 새 테이블 구성.
2. **문제 관점:** 수도권 제외 + 연계·동선이라는 지역균형 관점으로 문제 재정의.
3. **분석 관점:** 인기도 가중치를 반영한 **거리 기반 추천 동선** 알고리즘 직접 설계.
→ README/보고서에 위 기여와 참고자료 출처(과제 2.6)를 명시.

---

## 8. GitHub Repository 구조 (과제 2.3)

```
regional-tourism-bigdata/
├── README.md                 # 개요·실행방법(필수, 과제 2.4)
├── DESIGN.md                 # 본 설계서
├── report/                   # 최종 보고서·발표자료
├── src/
│   ├── collect/              # 수집 스크립트(Python/Bash)
│   ├── preprocess/           # Spark/Hive 전처리
│   ├── analyze/              # Spark SQL / HiveQL 분석
│   ├── mapreduce/            # MapReduce 보조 잡
│   └── visualize/            # 시각화
├── scripts/                  # hdfs_put.sh, run_all.sh 등 실행 스크립트
├── sql/                      # *.hql, *.sql 쿼리 모음
├── data/                     # (gitignore) raw/curated — 용량 큰 데이터 제외
├── results/figures/          # 결과 이미지·지도 HTML
├── conf/                     # 설정 예시(.env.example)
└── .gitignore
```
> 대용량 `data/`는 `.gitignore`로 제외하고, 재현은 수집 스크립트로 보장. 샘플 데이터만 소량 커밋.

---

## 9. 실행 환경 및 실행 방법 (과제 2.4 — GCP HDP Sandbox)

### 9.1 확정된 실행 환경 (수업 자료 09·10·11·12장 기준)
- **접속(중첩 SSH):** 로컬 PC → (PuTTY, 키+8080/30800 포트포워딩) → GCP VM(Ubuntu, 사용자 `gcptutorial`) → HDP 컨테이너 `ssh maria_dev@localhost -p 2222` (pw: `maria_dev`).
- **플랫폼:** HDP **3.0.1** (HDP 컨테이너=CentOS 7). HDFS 네임노드 `hdfs://sandbox-hdp.hortonworks.com:8020`, 사용자 홈 `/user/maria_dev`.
- **Spark 2.3.1** (`spark-submit`, pyspark 2.3 API — **Spark 3 전용 문법 금지**).
- **Hive 3.1.0** via **beeline**: `jdbc:hive2://sandbox-hdp.hortonworks.com:2181/default` (ZooKeeper 디스커버리). 웹 UI=Data Analytics Studio `localhost:30800`.
- **Python:** anaconda base(`python3`) + `python3.6`(`pip3.6`). **MapReduce는 `mrjob`** (`pip3.6 install mrjob`) 사용 — 강의에서 채택한 방식.
- **Pig:** `pig -f script` (기본 실행엔진 TEZ).
- **HDFS 조작:** `hadoop fs -ls/-put/-head/-cat`, 또는 Ambari Files View 업로드.

### 9.2 실행 순서 (README에 명시)
```bash
# 0) HDP 컨테이너 접속
ssh maria_dev@localhost -p 2222

# 1) 수집 (재실행 가능, 키는 환경변수)
export DATA_GO_KR_KEY="<공공데이터포털 서비스키>"
pip3.6 install requests          # 최초 1회
bash src/collect/run_collect.sh  # data/raw/*.json|csv 생성 + du -sh로 100MB 확인

# 2) HDFS 적재
hadoop fs -mkdir -p /user/maria_dev/tour/raw
hadoop fs -put -f data/raw/* /user/maria_dev/tour/raw/

# 3) 전처리: JSON 파싱·정제 (Spark 2.3)
spark-submit src/preprocess/build_curated.py
#   - spark.read.json 으로 중첩 API 응답 평탄화 → 좌표 double 변환·결측 제거·조인
#   - 결과를 Parquet 또는 Hive 외부테이블로 저장

# 4) 분석: HiveQL (beeline) / Spark SQL
beeline -u "jdbc:hive2://sandbox-hdp.hortonworks.com:2181/default" -f sql/q1_popularity.hql
spark-submit src/analyze/q3_route.py

# 5) MapReduce 보조 잡 (콘텐츠타입별 레코드 카운트)
python3.6 src/mapreduce/count_by_type.py -r hadoop hdfs:///user/maria_dev/tour/raw

# 6) 시각화
python3.6 src/visualize/make_figures.py
```

### 9.3 Hive 적재 패턴 (강의에서 배운 방식 채택)
강의에서는 CSV를 ① `CREATE TABLE temp (col_value STRING) STORED AS TEXTFILE` → ② `LOAD DATA INPATH ... OVERWRITE INTO TABLE temp` → ③ 타입 지정 테이블 생성 → ④ `regexp_extract`로 `insert overwrite` 하거나, `OpenCSVSerde`+`skip.header.line.count`로 적재했습니다. 본 프로젝트의 집계 CSV(C·D·E)는 이 패턴을 그대로 활용합니다.
> 주의: `LOAD DATA INPATH`는 HDFS 원본 파일을 **이동(삭제)** 합니다. 원본 보존이 필요하면 사본을 두거나 외부테이블(`EXTERNAL`)을 사용합니다.
> 관리 테이블 실데이터 경로: `hdfs:///warehouse/tablespace/managed/hive/`.

### 9.4 수집 위치에 관한 메모
HDP 컨테이너에서 외부 HTTP 다운로드가 동작함을 강의 실습(`wget`)에서 확인했습니다. 따라서 `apis.data.go.kr` 호출도 컨테이너에서 가능할 것으로 보이나(⚠️ 컨테이너 DNS가 간헐적으로 불안정한 사례 있음), 실패 시 **GCP VM(Ubuntu)에서 수집 후 컨테이너로 전송**하거나 로컬 수집 후 Ambari 업로드하는 대안을 README에 함께 기재합니다.

---

## 10. 일정 (제안)
| 주차/일자 | 작업 |
|---|---|
| 1단계 | C·D·E 명세 확정, A·B 추가 신청, 수집 스크립트 구현·100MB 확보 |
| 2단계 | HDFS 적재 + Spark/Hive 전처리 테이블 구축 |
| 3단계 | Q1~Q3 분석 쿼리·동선 알고리즘 구현 |
| 4단계 | 시각화·지도, README·보고서·발표자료 정리 |
> ⚠️ 제출 마감일을 알려주시면 일정을 날짜 기준으로 구체화하겠습니다.

---

## 11. 평가 요건 체크리스트 매핑
| 과제 요건 | 충족 설계 |
|---|---|
| 2.2 수집 단계 포함 | §5.1 수집 스크립트 |
| 2.2 누적 100MB | §3.3 전수 수집 + 일자 반복 |
| 2.2 수집 스크립트화·재실행 | §3.4 환경변수·인자·재시도 |
| 2.2 HDFS 적재(다양한 포맷) | §5.2 JSON/CSV→Parquet |
| 2.2 전처리(결측·타입·조인·집계) | §5.3 전부 포함 |
| 2.2 분석 3개 이상(GROUP BY/JOIN/통계) | §6 Q1~Q3+추가 |
| 2.3 단일 GitHub repo | §8 구조 |
| 2.4 Sandbox 실행·실행법 명시 | §9 README |
| 2.5 본인 기여 | §7 |
| 2.6 출처 명시 | §12 |

---

## 12. 참고자료·출처
- 한국관광공사 콘텐츠랩 / TourAPI 공식: https://api.visitkorea.or.kr/
- 한국관광공사_국문 관광정보 서비스_GW (data.go.kr ID 15101578): https://www.data.go.kr/data/15101578/openapi.do
- 한국관광공사_관광지별 연관 관광지 정보 (ID 15128560): https://www.data.go.kr/data/15128560/openapi.do
- 한국관광공사_관광빅데이터 정보서비스_GW (ID 15101972): https://www.data.go.kr/data/15101972/openapi.do
- 강의 자료: BDP 폴더 PDF (07·10 HDFS/MapReduce, 11 Pig&Hive·Spark, 12 Spark·Sqoop/Flume, 09 GCP)
> 구현 시 참고하는 코드·블로그·AI 도구 사용 내역은 README에 추가로 기록(과제 2.6).

---

## 13. 확인이 필요한 사항 (검토 요청)
1. **C·D·E 요청 파라미터·응답 필드** — Base URL·오퍼레이션은 확정(§3.1). 각 오퍼레이션의 정확한 파라미터/필드명만 `참고문서` zip 또는 샘플 호출로 확정하면 됩니다. (좌표 미포함은 확정)
2. ~~A(국문 관광정보)·B(연관 관광지) 추가 신청~~ → ✅ **완료** (15101578, 15128560). 분석 데이터 구성 확정.
3. **제출 마감일** — 일정 구체화용.
4. ~~HDP Sandbox 접속/버전~~ → ✅ **확인 완료**: HDP 3.0.1, Spark 2.3.1, Hive 3.1.0, `maria_dev` 중첩 SSH (§9 반영).
5. 시각화에서 **folium 지도(HTML)** 사용해도 되는지(정적 이미지만 필요한지).

> 위 항목 확정 후 구현 단계(수집 스크립트부터)로 진행하겠습니다.
