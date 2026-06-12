#!/usr/bin/env bash
# Spark 분석 실행 래퍼 — Spark가 Python3.6을 쓰도록 강제(HDP 기본은 Python2).
# 사용: bash scripts/run_spark.sh   (또는 인자로 입력 base 경로)
export PYSPARK_PYTHON=python3.6
export PYSPARK_DRIVER_PYTHON=python3.6
export PYTHONIOENCODING=utf-8
HERE="$(cd "$(dirname "$0")" && pwd)"
spark-submit "$HERE/../src/analyze/spark_analysis.py" "$@"
