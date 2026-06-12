#!/usr/bin/env bash
# 전체 수집 오케스트레이션 (HDP 컨테이너의 maria_dev 계정에서 실행)
# 사용: bash src/collect/run_collect.sh
set -e

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

# python3.6 우선, 없으면 python3
PY=python3.6
command -v $PY >/dev/null 2>&1 || PY=python3

# requests 설치(최초 1회) — 이미 있으면 통과
$PY -c "import requests" 2>/dev/null || pip3.6 install requests --user || pip3 install requests --user

echo "### 1/5 국문 관광정보(좌표 마스터) 수집"
$PY "$HERE/collect.py" tourapi

echo "### 2/5 방문자수(일자별) 수집"
$PY "$HERE/collect.py" visitor

echo "### 3/5 관광 자원 수요 수집"
$PY "$HERE/collect.py" demand

echo "### 4/5 관광 수요 강도 수집"
$PY "$HERE/collect.py" intensity

echo "### 5/5 연관 관광지(Tmap) 수집"
$PY "$HERE/collect.py" related

echo "### 누적 용량 확인"
du -sh "$ROOT/data/raw" || true
