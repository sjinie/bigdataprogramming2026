# bigdataprogramming2026

본 저장소는 **공개(public) GitHub repository**로 운영합니다.

## 문제 정의
- 수도권 집중 현상을 막기 위해 지방의 여러 관광지 중에서 인기 많은 관광지를 분석하고 이동 동선을 연계하는 데이터 분석 프로젝트를 수행합니다.
- 데이터는 한국관광공사 공공데이터와 공공데이터포털의 관광정보 데이터를 사용합니다.
- 방문자 수 기준 상위 지역 관광지를 도출하고, 관광지 간 이동 패턴을 분석해 연계 가능한 이동 동선 후보를 제시합니다.

## 기술 스택
- Python
- Docker
- HDFS
- Hive QL
- Kafka
- Spark
- scikit-learn
- seaborn

## 구현 계획
- 공공데이터 OpenAPI로 데이터 수집
- Kafka Connector로 HDFS에 저장
- Hive로 데이터 적재 및 분석용 테이블 구성
- Spark로 대용량 관광 데이터 전처리 및 집계 분석
- scikit-learn으로 관광지 인기/이동 패턴 모델링
- seaborn으로 분석 결과 시각화 및 인사이트 도출
