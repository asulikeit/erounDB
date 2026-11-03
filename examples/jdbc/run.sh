#!/usr/bin/env bash
# 사용: JAMSIL_USER=... JAMSIL_PASSWORD=... examples/jdbc/run.sh <host1,host2,host3>
set -euo pipefail
D="$(cd "$(dirname "$0")" && pwd)"
JAR="${PGJDBC_JAR:-$D/../../tests/jdbc/postgresql-42.7.4.jar}"
OUT="$(mktemp -d)"; trap 'rm -rf "$OUT"' EXIT
# javac 실행 파일이 없는 JRE+소스 실행기 환경도 지원: jdk.compiler 모듈을 직접 호출
JAVAC=(javac); command -v javac >/dev/null || JAVAC=(java -m jdk.compiler/com.sun.tools.javac.Main)
"${JAVAC[@]}" -d "$OUT" -cp "$JAR" "$D/JamsilClient.java" "$D/Quickstart.java"
java -cp "$OUT:$JAR" Quickstart "${1:-${JAMSIL_HOSTS:?hosts required}}"
