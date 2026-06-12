# -*- coding: utf-8 -*-
"""한국관광공사 OpenAPI 수집 공통 유틸: 서비스키 로드, HTTP GET(재시도),
표준 응답 파싱, 페이지네이션 전수 수집."""

import os
import sys
import io as _io
import json
import time
import urllib.parse

# 컨테이너 로캘이 ASCII인 경우 한글 출력이 깨지므로 stdout/stderr를 UTF-8로 래핑
for _name in ("stdout", "stderr"):
    _s = getattr(sys, _name, None)
    try:
        _enc = (_s.encoding or "").lower() if _s else "utf-8"
    except Exception:  # noqa
        _enc = ""
    if _s is not None and _enc not in ("utf-8", "utf8") and hasattr(_s, "buffer"):
        setattr(sys, _name, _io.TextIOWrapper(
            _s.buffer, encoding="utf-8", errors="replace", line_buffering=True))


def get_service_key():
    key = os.environ.get("DATA_GO_KR_KEY", "").strip()
    if not key or key.startswith("여기에"):
        sys.exit("환경변수 DATA_GO_KR_KEY 가 설정되지 않았습니다.")
    return key


def _key_is_encoded():
    return os.environ.get("DATA_GO_KR_KEY_IS_ENCODED", "0").strip() == "1"


def build_url(base_url, endpoint, params):
    base = base_url.rstrip("/") + "/" + endpoint.lstrip("/")
    key = get_service_key()
    qs = urllib.parse.urlencode(params, doseq=True)
    if _key_is_encoded():
        return "{0}?serviceKey={1}&{2}".format(base, key, qs)
    qs2 = urllib.parse.urlencode({"serviceKey": key}) + "&" + qs
    return "{0}?{1}".format(base, qs2)


def http_get_json(url, max_retries=4, timeout=30):
    import requests
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.encoding = "utf-8"
            if resp.status_code != 200:
                raise RuntimeError("HTTP {0}".format(resp.status_code))
            text = resp.text.strip()
            if text.startswith("<"):
                raise RuntimeError("XML 응답: " + text[:300])
            return json.loads(text)
        except Exception as e:  # noqa
            last_err = e
            wait = min(2 ** attempt, 20)
            sys.stderr.write("  재시도 {0}/{1}: {2}\n".format(attempt, max_retries, e))
            time.sleep(wait)
    raise RuntimeError("요청 실패: {0} ({1})".format(url, last_err))


def parse_standard(body_json):
    """표준 응답에서 (resultCode, resultMsg, items[], totalCount) 추출."""
    resp = body_json.get("response", {})
    header = resp.get("header", {})
    code = str(header.get("resultCode", ""))
    msg = header.get("resultMsg", "")
    body = resp.get("body", {}) or {}
    total = int(body.get("totalCount", 0) or 0)
    items_field = body.get("items", "")
    if not items_field:
        return code, msg, [], total
    item = items_field.get("item", []) if isinstance(items_field, dict) else []
    if isinstance(item, dict):
        item = [item]
    return code, msg, item, total


def ensure_dir(path):
    if not os.path.isdir(path):
        os.makedirs(path)


def dir_size_mb(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total / (1024.0 * 1024.0)


def fetch_all_pages(base_url, endpoint, params, out_dir, label,
                    num_of_rows=1000, max_pages=10000, sleep=0.2):
    """페이지네이션으로 전수 수집하여 페이지별 JSON 저장. 수집 건수 반환."""
    ensure_dir(out_dir)
    page, fetched, total = 1, 0, None
    while page <= max_pages:
        p = dict(params)
        p["numOfRows"] = num_of_rows
        p["pageNo"] = page
        body = http_get_json(build_url(base_url, endpoint, p))
        if isinstance(body, dict) and "OpenAPI_ServiceResponse" in body:
            cmm = body["OpenAPI_ServiceResponse"].get("cmmMsgHeader", {})
            sys.stderr.write("  에러 {0}: {1}\n".format(label, cmm.get("returnAuthMsg")))
            break
        code, msg, items, tot = parse_standard(body)
        if code not in ("0000", "00", ""):
            sys.stderr.write("  경고 {0} p{1}: {2} {3}\n".format(label, page, code, msg))
            break
        if not items:
            break
        out_path = os.path.join(out_dir, "{0}.p{1:04d}.json".format(label, page))
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(body, f, ensure_ascii=False)
        fetched += len(items)
        if total is None:
            total = tot
        sys.stdout.write("  {0} p{1}: +{2} ({3}/{4})\n".format(
            label, page, len(items), fetched, total))
        sys.stdout.flush()
        if total and fetched >= total:
            break
        page += 1
        time.sleep(sleep)
    return fetched
