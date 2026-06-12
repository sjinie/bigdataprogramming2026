# -*- coding: utf-8 -*-
"""한국관광공사 OpenAPI 수집기. 사용법은 README 참고."""

import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402
import config  # noqa: E402
import regions  # noqa: E402


def load_dotenv():
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.abspath(os.path.join(here, "..", ".."))
    for d in (root, os.getcwd(), here):
        path = os.path.join(d, ".env")
        if not os.path.isfile(path):
            continue
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                v = v.strip().strip('"').strip("'")
                if v and not v.startswith("여기에"):
                    os.environ[k.strip()] = v


def collect_tourapi(out_root, rows):
    cfg = config.KOR
    out_dir = os.path.join(out_root, "tourapi")
    grand = 0
    for area in config.AREA_CODES_LOCAL:
        for ctype in config.CONTENT_TYPE_IDS:
            params = dict(cfg["common_params"])
            params["areaCode"] = area
            params["contentTypeId"] = ctype
            label = "kor_a{0}_t{1}".format(area, ctype)
            grand += common.fetch_all_pages(cfg["base_url"], cfg["endpoint"],
                                            params, out_dir, label, num_of_rows=rows)
    return grand


def collect_multi(cfg, out_root, sub, rows, extra=None):
    out_dir = os.path.join(out_root, sub)
    grand = 0
    for ep in cfg["endpoints"]:
        params = dict(cfg["common_params"])
        if extra:
            params.update(extra)
        grand += common.fetch_all_pages(cfg["base_url"], ep, params,
                                        out_dir, ep, num_of_rows=rows)
    return grand


def collect_demand_like(cfg, out_root, sub, rows):
    """지방 시도 × baseYm 후보를 돌며 데이터가 있는 월을 수집.
    연속 3개월 빈 결과면 해당 지역은 건너뛴다."""
    out_dir = os.path.join(out_root, sub)
    grand = 0
    for area in regions.NON_CAPITAL_AREA_CDS:
        for ep in cfg["endpoints"]:
            got, zeros = 0, 0
            for ym in config.DEMAND_BASE_YMS:
                if got >= config.DEMAND_MONTHS_PER_AREA:
                    break
                params = dict(cfg["common_params"])
                params["areaCd"] = area
                params["baseYm"] = ym
                label = "{0}_a{1}_{2}".format(ep, area, ym)
                n = common.fetch_all_pages(cfg["base_url"], ep, params,
                                           out_dir, label, num_of_rows=rows)
                sys.stdout.write("  {0} a{1} {2}: {3}\n".format(ep, area, ym, n))
                sys.stdout.flush()
                if n > 0:
                    got += 1
                    grand += n
                    zeros = 0
                else:
                    zeros += 1
                    if got == 0 and zeros >= 3:
                        break
    return grand


def collect_related(out_root, rows):
    cfg = config.RELATED
    out_dir = os.path.join(out_root, "related")
    ym = config.RELATED_BASE_YM
    grand = 0
    for area in regions.NON_CAPITAL_AREA_CDS:
        for signgu_cd, _nm in regions.SIGUNGU.get(area, []):
            params = dict(cfg["common_params"])
            params["areaCd"] = area
            params["signguCd"] = signgu_cd
            params["baseYm"] = ym
            label = "rlte_{0}_{1}".format(signgu_cd, ym)
            grand += common.fetch_all_pages(cfg["base_url"], cfg["endpoint"],
                                            params, out_dir, label, num_of_rows=rows)
    return grand


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("source", choices=["tourapi", "visitor", "demand",
                                       "intensity", "related", "all"])
    ap.add_argument("--out", default=None)
    ap.add_argument("--rows", type=int, default=1000)
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    args = ap.parse_args()

    load_dotenv()
    common.get_service_key()

    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.abspath(os.path.join(here, "..", ".."))
    out_root = args.out or os.path.join(root, "data", "raw")
    common.ensure_dir(out_root)

    vis_extra = {}
    if args.start:
        vis_extra["startYmd"] = args.start
    if args.end:
        vis_extra["endYmd"] = args.end

    todo = [args.source] if args.source != "all" else \
        ["tourapi", "visitor", "demand", "intensity", "related"]

    for src in todo:
        print("==== {0} ====".format(src))
        if src == "tourapi":
            collect_tourapi(out_root, args.rows)
        elif src == "visitor":
            collect_multi(config.VISITOR, out_root, "visitor", args.rows, vis_extra)
        elif src == "demand":
            collect_demand_like(config.RES_DEMAND, out_root, "demand", args.rows)
        elif src == "intensity":
            collect_demand_like(config.DEM_INTENSITY, out_root, "intensity", args.rows)
        elif src == "related":
            collect_related(out_root, args.rows)

    size = common.dir_size_mb(out_root)
    print("==== 완료: {0:.1f} MB ({1}) ====".format(size, out_root))


if __name__ == "__main__":
    main()
