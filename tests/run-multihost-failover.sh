#!/usr/bin/env bash
# 다중 호스트 장애 전환 시험 실행기 (샘플 클라이언트 + 시험을 함께 컴파일). 사용: tests/run-multihost-failover.sh <ip1,ip2,ip3>
set -euo pipefail
R="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$R/tests/jdbc/postgresql-42.7.4.jar"
OUT="$(mktemp -d)"; trap 'rm -rf "$OUT"' EXIT
JAVAC=(javac); command -v javac >/dev/null || JAVAC=(java -m jdk.compiler/com.sun.tools.javac.Main)
"${JAVAC[@]}" -d "$OUT" -cp "$JAR" "$R/examples/jdbc/JamsilClient.java" "$R/tests/jdbc/MultiHostFailover.java"
java -cp "$OUT:$JAR" MultiHostFailover "${1:?hosts required}"
