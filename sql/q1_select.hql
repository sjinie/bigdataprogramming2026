-- 이미 적재된 visitor 테이블에서 계절별 통계만 다시 조회(재표시용).
-- 한글이 깨지면 beeline 실행 전:  export HADOOP_CLIENT_OPTS="-Dfile.encoding=UTF-8"
-- 또는 DAS 웹UI(localhost:30800)에서 실행하면 한글이 정상 표시됨.
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
  AND touDivCd IN ('2','3')
  AND areaCode NOT IN ('11','28','41')
GROUP BY regionNm,
  CASE
    WHEN SUBSTR(baseYmd,5,2) IN ('03','04','05') THEN '1.봄'
    WHEN SUBSTR(baseYmd,5,2) IN ('06','07','08') THEN '2.여름'
    WHEN SUBSTR(baseYmd,5,2) IN ('09','10','11') THEN '3.가을'
    ELSE '4.겨울'
  END
ORDER BY regionNm, season;
