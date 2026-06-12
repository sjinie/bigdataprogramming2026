# -*- coding: utf-8 -*-
"""분석 결과 CSV로 folium 지도(map_folium.html) 생성.
빨간 원=허브 관광지, 파란 선=추천 동선, 회색 점선=MST."""
import os
import csv
import sys
import io as _io

for _n in ("stdout", "stderr"):
    _s = getattr(sys, _n, None)
    if _s is not None and hasattr(_s, "buffer"):
        try:
            if (_s.encoding or "").lower() not in ("utf-8", "utf8"):
                setattr(sys, _n, _io.TextIOWrapper(_s.buffer, encoding="utf-8",
                                                   errors="replace", line_buffering=True))
        except Exception:  # noqa
            pass

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
RES = os.path.join(ROOT, "data", "results")


def read_csv(name):
    p = os.path.join(RES, name)
    if not os.path.isfile(p):
        return []
    with open(p, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fnum(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def main():
    try:
        import folium
    except ImportError:
        sys.exit("[오류] folium 미설치. 'pip3.6 install folium' 후 다시 실행하세요.")

    markers = read_csv("map_markers.csv")
    route = read_csv("q3_route.csv")
    mst = read_csv("q3_mst.csv")

    m = folium.Map(location=[36.3, 127.8], zoom_start=7, tiles="OpenStreetMap")

    # 1) 최단연결트리(MST) — 회색 점선
    for e in mst:
        a = [fnum(e.get("from_lat")), fnum(e.get("from_lon"))]
        b = [fnum(e.get("to_lat")), fnum(e.get("to_lon"))]
        if None in a or None in b:
            continue
        folium.PolyLine([a, b], color="#7f8c8d", weight=1.5, opacity=0.6,
                        dash_array="5").add_to(m)

    # 2) 허브 관광지 — 빨간 원(연결성에 따라 크기)
    for r in markers:
        lat, lon = fnum(r.get("lat")), fnum(r.get("lon"))
        if lat is None or lon is None:
            continue
        conn = fnum(r.get("connectivity")) or 0
        folium.CircleMarker(
            [lat, lon], radius=3 + min(conn, 80) / 20.0,
            color="#e74c3c", fill=True, fill_color="#e74c3c", fill_opacity=0.7,
            weight=1, popup=folium.Popup("{0} (연결성 {1})".format(
                r.get("name", ""), int(conn)), max_width=250)
        ).add_to(m)

    # 3) 추천 동선 — 파란 선 + 순번 마커
    pts = []
    for r in sorted(route, key=lambda x: int(x.get("order") or 0)):
        lat, lon = fnum(r.get("lat")), fnum(r.get("lon"))
        if lat is None or lon is None:
            continue
        pts.append([lat, lon])
        folium.Marker(
            [lat, lon],
            icon=folium.DivIcon(html=(
                '<div style="background:#2980b9;color:#fff;border-radius:50%;'
                'width:22px;height:22px;line-height:22px;text-align:center;'
                'font-size:12px;font-weight:bold">{0}</div>'.format(r.get("order")))),
            popup="{0}. {1}".format(r.get("order"), r.get("spot", ""))
        ).add_to(m)
    if len(pts) >= 2:
        folium.PolyLine(pts, color="#2980b9", weight=3, opacity=0.85).add_to(m)
        m.fit_bounds(pts)

    # 범례
    legend = ('<div style="position:fixed;bottom:24px;left:24px;z-index:9999;'
              'background:#fff;padding:10px 12px;border-radius:8px;font-size:13px;'
              'box-shadow:0 1px 6px rgba(0,0,0,.3)">'
              '<b>비수도권 관광 허브 · 추천 동선</b><br>'
              '<span style="color:#e74c3c">●</span> 인기 관광지(허브)&nbsp;'
              '<span style="color:#2980b9">▬</span> 추천 동선&nbsp;'
              '<span style="color:#7f8c8d">┄</span> 최단연결트리(MST)</div>')
    m.get_root().html.add_child(folium.Element(legend))

    if not os.path.isdir(RES):
        os.makedirs(RES)
    out = os.path.join(RES, "map_folium.html")
    m.save(out)
    print("생성: {0} (마커 {1} / 동선 {2} / MST {3})".format(
        out, len(markers), len(pts), len(mst)))


if __name__ == "__main__":
    main()
