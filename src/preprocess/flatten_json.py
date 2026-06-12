# -*- coding: utf-8 -*-
"""원시 API JSON을 분석용 CSV(poi/visitor/related)로 평탄화."""
import os
import csv
import sys
import glob
import json
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
RAW = os.path.join(ROOT, "data", "raw")
CUR = os.path.join(ROOT, "data", "curated")


def iter_items(sub):
    for fp in sorted(glob.glob(os.path.join(RAW, sub, "*.json"))):
        try:
            with open(fp, encoding="utf-8") as f:
                body = json.load(f)
        except Exception:  # noqa
            continue
        items = body.get("response", {}).get("body", {}).get("items", "")
        if not items:
            continue
        it = items.get("item", []) if isinstance(items, dict) else []
        if isinstance(it, dict):
            it = [it]
        for row in it:
            if isinstance(row, dict):
                yield row


def g(row, *keys):
    for k in keys:
        v = row.get(k)
        if v not in (None, ""):
            return str(v).strip()
    return ""


def ensure_cur():
    if not os.path.isdir(CUR):
        os.makedirs(CUR)


def write_csv(name, header, rows):
    ensure_cur()
    path = os.path.join(CUR, name)
    n = 0
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)
            n += 1
    print("  생성: {0}  ({1} 행)".format(path, n))
    return n


def build_poi():
    cols = ["contentid", "title", "contenttypeid", "areacode", "sigungucode",
            "cat1", "cat2", "cat3", "mapx", "mapy", "addr1",
            "lDongRegnCd", "lDongSignguCd"]

    def rows():
        for r in iter_items("tourapi"):
            yield [g(r, "contentid"), g(r, "title"), g(r, "contenttypeid"),
                   g(r, "areacode"), g(r, "sigungucode"),
                   g(r, "cat1"), g(r, "cat2"), g(r, "cat3"),
                   g(r, "mapx"), g(r, "mapy"), g(r, "addr1"),
                   g(r, "lDongRegnCd"), g(r, "lDongSignguCd")]
    return write_csv("poi.csv", cols, rows())


def build_visitor():
    cols = ["level", "areaCode", "regionCode", "regionNm", "baseYmd",
            "daywkDivCd", "daywkDivNm", "touDivCd", "touDivNm", "touNum"]

    # metco(광역)/locgo(기초)를 파일명으로 구분해 처리
    def gen():
        for fp in sorted(glob.glob(os.path.join(RAW, "visitor", "*.json"))):
            base = os.path.basename(fp)
            is_metco = base.startswith("metco")
            try:
                with open(fp, encoding="utf-8") as f:
                    body = json.load(f)
            except Exception:
                continue
            items = body.get("response", {}).get("body", {}).get("items", "")
            if not items:
                continue
            it = items.get("item", []) if isinstance(items, dict) else []
            if isinstance(it, dict):
                it = [it]
            for r in it:
                if is_metco:
                    code = g(r, "areaCode")
                    yield ["sido", code, code, g(r, "areaNm"), g(r, "baseYmd"),
                           g(r, "daywkDivCd"), g(r, "daywkDivNm"),
                           g(r, "touDivCd"), g(r, "touDivNm"), g(r, "touNum")]
                else:
                    sc = g(r, "signguCode")
                    yield ["sigungu", sc[:2], sc, g(r, "signguNm"), g(r, "baseYmd"),
                           g(r, "daywkDivCd"), g(r, "daywkDivNm"),
                           g(r, "touDivCd"), g(r, "touDivNm"), g(r, "touNum")]
    return write_csv("visitor.csv", cols, gen())


def build_related():
    cols = ["baseYm", "areaCd", "areaNm", "signguCd", "signguNm",
            "tAtsCd", "tAtsNm", "rlteTatsCd", "rlteTatsNm",
            "rlteRegnCd", "rlteRegnNm", "rlteSignguCd", "rlteSignguNm",
            "rlteCtgryLclsNm", "rlteCtgryMclsNm", "rlteCtgrySclsNm", "rlteRank"]

    def rows():
        for r in iter_items("related"):
            yield [g(r, "baseYm"), g(r, "areaCd"), g(r, "areaNm"),
                   g(r, "signguCd"), g(r, "signguNm"),
                   g(r, "tAtsCd"), g(r, "tAtsNm"),
                   g(r, "rlteTatsCd"), g(r, "rlteTatsNm"),
                   g(r, "rlteRegnCd"), g(r, "rlteRegnNm"),
                   g(r, "rlteSignguCd"), g(r, "rlteSignguNm"),
                   g(r, "rlteCtgryLclsNm"), g(r, "rlteCtgryMclsNm"),
                   g(r, "rlteCtgrySclsNm"), g(r, "rlteRank")]
    return write_csv("related.csv", cols, rows())


def main():
    print("== 평탄화 시작 ==")
    build_poi()
    build_visitor()
    build_related()
    print("== 완료: data/curated/*.csv ==")


if __name__ == "__main__":
    main()
