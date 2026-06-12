-- Q1 (Hive) : 비수도권 시군구의 계절별 방문자 통계 (GROUP BY + 평균/표준편차)
-- 수업에서 배운 방식(OpenCSVSerde + skip header + LOAD DATA INPATH)을 그대로 사용.
--
-- 실행: CREATE 권한이 있는 'hive' 계정으로 접속해야 함(maria_dev는 default DB CREATE 권한 없음 - Ranger).
--   1) Hive용 사본을 만들고 hive가 옮길 수 있게 권한 개방 (maria_dev 셸에서):
--        hadoop fs -mkdir -p /user/maria_dev/tour/hive_src
--        hadoop fs -cp -f /user/maria_dev/tour/curated/visitor/visitor.csv /user/maria_dev/tour/hive_src/
--        hadoop fs -chmod -R 777 /user/maria_dev/tour/hive_src
--   2) hive 계정으로 접속해 실행:
--        beeline -u "jdbc:hive2://localhost:10000/default" -n hive
--        0: jdbc:hive2://...> !run sql/q1_hub_seasonal.hql
--   (또는 Data Analytics Studio 웹UI localhost:30800 에 아래 SQL 붙여넣기)

DROP TABLE IF EXISTS visitor;

CREATE TABLE IF NOT EXISTS visitor (
  level STRING, areaCode STRING, regionCode STRING, regionNm STRING, baseYmd STRING,
  daywkDivCd STRING, daywkDivNm STRING, touDivCd STRING, touDivNm STRING, touNum STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/maria_dev/tour/hive_src/visitor.csv' OVERWRITE INTO TABLE visitor;

-- 계절별 시군구 외지인+외국인 일평균 방문자수 / 표준편차
SELECT
  regionNm,
  CASE
    WHEN SUBSTR(baseYmd,5,2) IN ('03','04','05') THEN '1.봄'
    WHEN SUBSTR(baseYmd,5,2) IN ('06','07','08') THEN '2.여름'
    WHEN SUBSTR(baseYmd,5,2) IN ('09','10','11') THEN '3.가을'
    ELSE '4.겨울'
  END AS season,
  ROUND(AVG(CAST(touNum AS DOUBLE)))        AS avg_visitors,
  ROUND(STDDEV_POP(CAST(touNum AS DOUBLE))) AS std_visitors,
  COUNT(*)                                  AS rows_cnt
FROM visitor
WHERE level = 'sigungu'
  AND touDivCd IN ('2','3')                 -- 외지인 + 외국인
  AND areaCode NOT IN ('11','28','41')      -- 수도권 제외
GROUP BY regionNm,
  CASE
    WHEN SUBSTR(baseYmd,5,2) IN ('03','04','05') THEN '1.봄'
    WHEN SUBSTR(baseYmd,5,2) IN ('06','07','08') THEN '2.여름'
    WHEN SUBSTR(baseYmd,5,2) IN ('09','10','11') THEN '3.가을'
    ELSE '4.겨울'
  END
ORDER BY regionNm, season;
