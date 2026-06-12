# 비수도권 관광 허브 발굴 및 Spark/Hive 기반 다중 목적지 경로 추천 시스템

빅데이터 프로그래밍 기말 프로젝트. 한국관광공사 공개 API로 수집한 데이터를 Hadoop(HDFS/Hive/Spark)
위에서 분석하여, **비수도권 핵심 관광 허브를 발굴**하고, **허브-배후 인프라의 연계성**을 분석하며,
**허브를 중심으로 한 다중 목적지 추천 동선(네트워크)**을 도출합니다. 설계 상세는 [`DESIGN.md`](DESIGN.md) 참고.

## 데이터 소스 (data.go.kr, 한국관광공사 · 전부 활용신청 완료)
| 역할 | 데이터(ID) | 서비스 |
|---|---|---|
| 좌표 마스터 | 국문 관광정보(15101578) | `KorService2/areaBasedList2` |
| 인기도(일별) | 방문자수(15101972) | `DataLabService/metco·locgoRegnVisitrDDList` |
| 인기도 | 관광 자원 수요(15152138) | `AreaTarResDemService` |
| 인기도 | 관광 수요 강도(15151868) | `AreaTarDemDsService` |
| 연계·동선 | 연관 관광지(15128560) | `TarRlteTarService1/areaBasedList1` |

## 실행 환경
GCP VM → `ssh maria_dev@localhost -p 2222` (HDP 3.0.1). Spark 2.3.1, Hive 3.1.0(beeline), python3.6, `hadoop fs`.

---

## 1단계 — 데이터 수집 (구현 완료)

재실행 가능한 Python 수집기로 5개 API를 누적 100MB 이상 수집합니다.

### 1.1 키 설정
```bash
cp .env.example .env
vi .env          # DATA_GO_KR_KEY 에 공공데이터포털 '일반 인증키(Decoding)' 붙여넣기
```

### 1.2 실행
```bash
# 전체 한 번에
bash src/collect/run_collect.sh

# 또는 소스별로
python3.6 src/collect/collect.py tourapi     # 국문 관광정보(지방 전수, 좌표)
python3.6 src/collect/collect.py visitor      # 방문자수(일별)  --start 20250101 --end 20250531
python3.6 src/collect/collect.py demand       # 관광 자원 수요
python3.6 src/collect/collect.py intensity    # 관광 수요 강도
python3.6 src/collect/collect.py related       # 연관 관광지(Tmap)
```
- 원시 JSON은 `data/raw/<source>/` 에 페이지 단위로 저장됩니다(재실행 시 덮어쓰기).
- 종료 시 누적 용량(MB)을 출력하며, 100MB 미만이면 기간/콘텐츠타입을 넓히라고 안내합니다.
- 키·파라미터 오류 시 API의 `resultMsg`를 그대로 출력하므로 바로 수정할 수 있습니다.

### 1.3 구성
```
src/collect/
├── common.py       # HTTP 재시도·페이지네이션·표준응답 파싱·저장·용량로깅
├── config.py       # 5개 API 엔드포인트·파라미터·지방 areaCode·콘텐츠타입
├── collect.py      # 서브커맨드(tourapi/visitor/demand/intensity/related/all)
└── run_collect.sh  # 전체 오케스트레이션
```

> 참고: 표준 파라미터와 국문 관광정보 파라미터는 확정. 방문자수/수요/연관 관광지의
> 일부 파라미터(기간·지역코드)는 각 데이터 '참고문서(zip)' 기준이며 첫 호출로 검증합니다.
> 코드 한 곳(`config.py`)만 고치면 되도록 분리해 두었습니다.

---

## 2단계 — 전처리(평탄화) + HDFS 적재

```bash
# (a) 원시 JSON → 정제 CSV (data/curated/poi.csv, visitor.csv, related.csv)
export PYTHONIOENCODING=utf-8
python3.6 src/preprocess/flatten_json.py

# (b) 원시 + 정제본을 HDFS에 적재 (정제본은 소스별 서브디렉터리)
bash scripts/hdfs_put.sh                       # 원시 JSON → /user/maria_dev/tour/raw
for s in poi visitor related; do
  hadoop fs -mkdir -p /user/maria_dev/tour/curated/$s
  hadoop fs -put -f data/curated/$s.csv /user/maria_dev/tour/curated/$s/
done
```

## 3단계 — 분석 (Hive + Spark, 핵심 질문 3개)

```bash
# Q1(계절별 허브 통계) — Hive(HiveQL, GROUP BY + 평균/표준편차)
#  ① Hive용 사본(권한 개방) — maria_dev 셸에서
hadoop fs -mkdir -p /user/maria_dev/tour/hive_src
hadoop fs -cp -f /user/maria_dev/tour/curated/visitor/visitor.csv /user/maria_dev/tour/hive_src/
hadoop fs -chmod -R 777 /user/maria_dev/tour/hive_src
#  ② CREATE 권한 있는 'hive' 계정으로 접속해 실행 (maria_dev는 Ranger상 default DB CREATE 불가)
beeline -u "jdbc:hive2://localhost:10000/default" -n hive
#  프롬프트에서:  !run sql/q1_hub_seasonal.hql   (또는 DAS 웹UI localhost:30800)

# Q1(허브 발굴)·Q2(인프라 상관)·Q3(네트워크 동선) — Spark
bash scripts/run_spark.sh
```
- **Q1 (허브 발굴, GROUP BY/통계)**: Tmap 연결성으로 비수도권 핵심 관광 허브(관광지/시군구) 발굴 + Hive로 계절별 방문자 평균·표준편차.
- **Q2 (연계·상관, JOIN/통계)**: 허브 반경 5km 내 배후 인프라(숙박·음식·쇼핑) 밀집도와 허브 인기도의 **상관계수**(거리 기반 JOIN).
- **Q3 (네트워크 동선, 그래프)**: 허브 지역 관광지를 노드·거리를 에지로 한 **최단연결트리(MST)** + 허브 기점 **최근접 추천 동선**.
- 결과는 콘솔에 표로 출력(SSH 확인)되고, 시각화용 CSV가 `data/results/`에 저장됩니다.

## 4단계 — 시각화

**(a) 지도(folium)** — 인기 허브·추천 동선·MST를 대한민국 지도에 표시 (컨테이너 웹서버로 호스팅)
```bash
pip3.6 install folium
python3.6 src/visualize/make_map_folium.py     # → data/results/map_folium.html
cd data/results && python3.6 -m http.server 8888
# ssh에 8888 포트포워딩 추가 후 브라우저: http://localhost:8888/map_folium.html
```

**(b) 차트(matplotlib)** — Spark 집계 결과를 PNG 차트로 (라벨이 모두 영어/로마자라 **한글 폰트 불필요**)
```bash
pip3.6 install matplotlib numpy
python3.6 src/visualize/make_charts.py          # → data/results/figures/*.png
```
- fig1 허브 시군구 Top10 / fig2 인기 관광지 Top12 / fig3 인기도-인프라 산점도(Pearson r) / fig4 월별 추이
- 지역·관광지명은 로마자로 자동 변환(예: 제주시→Jejusi)되어 어떤 환경에서도 라벨이 깨지지 않습니다.

## 분석 질문 요약
1. **허브 발굴**: 비수도권에서 핵심 관광 허브(관광지·시군구)는 어디인가? — Tmap 연결성 집계 + Hive 계절별 방문자 통계(평균·표준편차)
2. **연계·상관**: 허브의 인기도는 반경 5km 배후 인프라(숙박·음식·쇼핑) 밀집도와 어떤 상관을 갖는가? — 거리 기반 JOIN + Pearson 상관계수
3. **네트워크 동선**: 허브를 중심으로 주변 관광지를 잇는 최적 연계 동선은? — 관광지=노드/거리=에지, 최단연결트리(MST) + 최근접 추천 동선

> 본인 기여(명세 2.5): 방문자수는 업무·통근이 섞여 관광 인기를 왜곡하므로, 실제 함께 방문 패턴(Tmap 연결성)으로 '관광 허브'를 재정의하고, 허브-인프라 상관과 네트워크 동선으로 지역 균형 관광 관점을 더함.

## 출처
- 한국관광공사 콘텐츠랩 https://api.visitkorea.or.kr/ · 한국관광 데이터랩 https://datalab.visitkorea.or.kr/
- 강의 자료(BDP): GCP Setup, MapReduce/HDFS, Pig & Hive, Spark, Sqoop/Flume
