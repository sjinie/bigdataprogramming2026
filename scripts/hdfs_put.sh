#!/usr/bin/env bash
# data/raw 의 원시 JSON을 HDFS에 적재한다 (HDP 컨테이너 maria_dev 에서 실행).
# 사용: bash scripts/hdfs_put.sh
set -e

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
HDFS_BASE=/user/maria_dev/tour/raw

echo "### 로컬 용량"
du -sh "$ROOT/data/raw" || true

echo "### HDFS 디렉터리 생성"
hadoop fs -mkdir -p "$HDFS_BASE"

echo "### 적재 (tourapi/visitor/demand/intensity/related)"
for sub in "$ROOT"/data/raw/*/ ; do
  name=$(basename "$sub")
  hadoop fs -mkdir -p "$HDFS_BASE/$name"
  hadoop fs -put -f "$sub"*.json "$HDFS_BASE/$name/" 2>/dev/null || \
    echo "  ($name: 파일 없음 또는 적재 생략)"
done

echo "### HDFS 확인"
hadoop fs -du -s -h "$HDFS_BASE"/* || true
echo "완료: $HDFS_BASE"
