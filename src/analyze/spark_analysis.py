# -*- coding: utf-8 -*-
"""Spark 분석: 허브 발굴(Q1), 인프라 상관(Q2), 네트워크 동선(Q3).
HDFS의 정제 CSV(poi/visitor/related)를 읽어 분석하고 결과를 data/results/에 저장한다."""
import os
import sys
import csv
import math

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
RESULTS = os.path.join(ROOT, "data", "results")
CAP = "('11','28','41')"        # 수도권 행정코드(서울11·인천28·경기41)
CAP_TOUR = "('1','2','31')"     # 수도권 TourAPI areaCode(서울1·인천2·경기31)


def save_rows(name, header, rows):
    if not os.path.isdir(RESULTS):
        os.makedirs(RESULTS)
    path = os.path.join(RESULTS, name)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(list(r))
    print("  저장: {0} ({1})".format(path, len(rows)))


def show(t):
    print("\n" + "=" * 72 + "\n" + t + "\n" + "=" * 72)


def hav(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    x = (math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(x))


def nn_route(nodes):
    """연결성 최고점을 기점으로 한 최근접(greedy) 동선."""
    if not nodes:
        return []
    pts = sorted(nodes, key=lambda p: -p[3])
    route = [pts.pop(0)]
    while pts:
        last = route[-1]
        nxt = min(pts, key=lambda p: hav(last[1], last[2], p[1], p[2]))
        pts.remove(nxt)
        route.append(nxt)
    return route


def pearson(xs, ys):
    n = len(xs)
    if n < 2:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    vx = sum((x - mx) ** 2 for x in xs)
    vy = sum((y - my) ** 2 for y in ys)
    if vx == 0 or vy == 0:
        return None
    return cov / math.sqrt(vx * vy)


def prim_mst(nodes):
    """Prim 알고리즘으로 최단연결트리 에지 [(i, j, dist_km)] 계산."""
    n = len(nodes)
    if n < 2:
        return []
    used = [False] * n
    used[0] = True
    edges = []
    for _ in range(n - 1):
        best = None
        for i in range(n):
            if not used[i]:
                continue
            for j in range(n):
                if used[j]:
                    continue
                d = hav(nodes[i][1], nodes[i][2], nodes[j][1], nodes[j][2])
                if best is None or d < best[2]:
                    best = (i, j, d)
        if best is None:
            break
        used[best[1]] = True
        edges.append(best)
    return edges


def main():
    base = sys.argv[1] if len(sys.argv) > 1 else "hdfs:///user/maria_dev/tour/curated"
    spark = SparkSession.builder.appName("regional-tourism-hub").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    def rd(n):
        return spark.read.option("header", True).csv(base.rstrip("/") + "/" + n)

    related = rd("related")
    poi = (rd("poi")
           .withColumn("mapx", F.col("mapx").cast("double"))
           .withColumn("mapy", F.col("mapy").cast("double")))
    related.createOrReplaceTempView("related")
    poi.createOrReplaceTempView("poi")

    spark.sql("""
        SELECT *, CONCAT(rlteRegnNm,' ',rlteSignguNm) AS rlteRegion
        FROM related
        WHERE areaCd NOT IN {cap}
          AND SUBSTRING(rlteSignguCd,1,2) NOT IN {cap}
          AND rlteTatsNm <> ''
    """.format(cap=CAP)).createOrReplaceTempView("rel_local")

    # Q1. 허브 발굴 (Tmap 연결성)
    show("Q1-1. 비수도권 핵심 관광 허브 Top20")
    q1 = spark.sql("""
        SELECT rlteTatsNm AS spot, FIRST(rlteRegion) AS region,
               FIRST(rlteCtgryMclsNm) AS category, COUNT(DISTINCT tAtsNm) AS connectivity
        FROM rel_local GROUP BY rlteTatsNm ORDER BY connectivity DESC LIMIT 20
    """)
    q1.show(20, truncate=False)
    save_rows("q1_top_spots.csv", ["spot", "region", "category", "connectivity"],
              [(r["spot"], r["region"], r["category"], r["connectivity"]) for r in q1.collect()])

    show("Q1-2. 비수도권 핵심 허브 시군구 Top15")
    q1b = spark.sql("""
        WITH s AS (SELECT rlteTatsNm, FIRST(rlteRegion) AS region,
                          COUNT(DISTINCT tAtsNm) AS conn
                   FROM rel_local GROUP BY rlteTatsNm)
        SELECT region, SUM(conn) AS conn_sum, COUNT(*) AS n_spots
        FROM s GROUP BY region ORDER BY conn_sum DESC LIMIT 15
    """)
    q1b.show(15, truncate=False)
    save_rows("q1_top_regions.csv", ["region", "conn_sum", "n_spots"],
              [(r["region"], r["conn_sum"], r["n_spots"]) for r in q1b.collect()])

    target = q1b.collect()[0]["region"] if q1b.count() else None

    # Q2. 허브 인기도 ↔ 반경 5km 배후 인프라 상관
    show("Q2. 허브 인기도 ↔ 반경 5km 배후 인프라 상관")
    spark.sql("""
        WITH s AS (SELECT rlteTatsNm AS name, COUNT(DISTINCT tAtsNm) AS conn
                   FROM rel_local GROUP BY rlteTatsNm)
        SELECT s.name, s.conn, p.mapy AS lat, p.mapx AS lon
        FROM s JOIN poi p ON p.title = s.name
        WHERE p.mapx IS NOT NULL
        ORDER BY s.conn DESC LIMIT 50
    """).createOrReplaceTempView("hub")
    spark.sql("""
        SELECT contenttypeid, mapy AS lat, mapx AS lon FROM poi
        WHERE mapx IS NOT NULL AND contenttypeid IN ('32','38','39')
          AND areacode NOT IN {capt}
    """.format(capt=CAP_TOUR)).createOrReplaceTempView("infra")
    # CROSS JOIN 후 haversine 거리 5km 이내만 집계
    infra_cnt = spark.sql("""
        SELECT h.name, h.conn,
               SUM(CASE WHEN i.contenttypeid='32' THEN 1 ELSE 0 END) AS stay,
               SUM(CASE WHEN i.contenttypeid='39' THEN 1 ELSE 0 END) AS food,
               SUM(CASE WHEN i.contenttypeid='38' THEN 1 ELSE 0 END) AS shop,
               COUNT(*) AS infra_total
        FROM hub h CROSS JOIN infra i
        WHERE 6371.0 * 2 * ASIN(SQRT(
                POWER(SIN(RADIANS(i.lat - h.lat) / 2), 2) +
                COS(RADIANS(h.lat)) * COS(RADIANS(i.lat)) *
                POWER(SIN(RADIANS(i.lon - h.lon) / 2), 2))) <= 5.0
        GROUP BY h.name, h.conn
        ORDER BY infra_total DESC
    """)
    res = infra_cnt.collect()
    infra_cnt.show(20, truncate=False)
    save_rows("q2_hub_infra.csv", ["hub", "connectivity", "stay", "food", "shop", "infra_total"],
              [(r["name"], r["conn"], r["stay"], r["food"], r["shop"], r["infra_total"]) for r in res])
    corr = pearson([float(r["conn"]) for r in res], [float(r["infra_total"]) for r in res])
    print("\n  Pearson r = {0}".format(round(corr, 3) if corr is not None else "N/A"))

    # Q3. 네트워크 동선 (MST + 최근접 동선)
    show("Q3. 허브 지역 네트워크 — 최단연결트리(MST) + 추천 동선")
    if target:
        tr = target.replace("'", "''")
        cand = spark.sql("""
            WITH s AS (SELECT rlteTatsNm AS name, COUNT(DISTINCT tAtsNm) AS conn
                       FROM rel_local WHERE rlteRegion='{tr}' GROUP BY rlteTatsNm)
            SELECT s.name, s.conn, p.mapy AS lat, p.mapx AS lon
            FROM s JOIN poi p ON p.title = s.name
            WHERE p.mapx IS NOT NULL
            GROUP BY s.name, s.conn, p.mapy, p.mapx
            ORDER BY s.conn DESC LIMIT 12
        """.format(tr=tr)).collect()
        nodes = [(r["name"], float(r["lat"]), float(r["lon"]), int(r["conn"])) for r in cand]
        print("  대상 지역: {0} (노드 {1})".format(target, len(nodes)))

        mst = prim_mst(nodes)
        print("  MST 총 길이 {0:.1f} km".format(sum(e[2] for e in mst)))
        mst_rows = []
        for i, j, d in mst:
            print("    {0} -- {1:.1f}km -- {2}".format(nodes[i][0], d, nodes[j][0]))
            mst_rows.append((nodes[i][0], nodes[i][1], nodes[i][2],
                             nodes[j][0], nodes[j][1], nodes[j][2], round(d, 2)))
        save_rows("q3_mst.csv",
                  ["from", "from_lat", "from_lon", "to", "to_lat", "to_lon", "dist_km"], mst_rows)

        route = nn_route(nodes)
        r_rows = []
        for k, (nm, lat, lon, conn) in enumerate(route, 1):
            print("    {0}. {1} ({2})".format(k, nm, conn))
            r_rows.append((target, k, nm, lon, lat, conn))
        save_rows("q3_route.csv", ["region", "order", "spot", "lon", "lat", "connectivity"], r_rows)

        markers = spark.sql("""
            WITH s AS (SELECT rlteTatsNm AS name, COUNT(DISTINCT tAtsNm) AS conn
                       FROM rel_local WHERE rlteRegion='{tr}' GROUP BY rlteTatsNm)
            SELECT DISTINCT s.name, p.mapx AS lon, p.mapy AS lat, s.conn
            FROM s JOIN poi p ON p.title = s.name WHERE p.mapx IS NOT NULL
        """.format(tr=tr))
        save_rows("map_markers.csv", ["name", "lon", "lat", "connectivity"],
                  [(r["name"], r["lon"], r["lat"], r["conn"]) for r in markers.collect()])

    spark.stop()


if __name__ == "__main__":
    main()
