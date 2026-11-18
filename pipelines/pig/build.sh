#!/usr/bin/env bash
set -euo pipefail

: "${JAVA_HOME:?Set JAVA_HOME to JDK 11}"
: "${HADOOP_HOME:?Set HADOOP_HOME to Hadoop 3.4.3}"
: "${PIG_HOME:?Set PIG_HOME to the Pig 0.18.0 Hadoop 3 distribution}"

cd "$(dirname "$0")"
mkdir -p build
tmp=$(mktemp -d build/compile.XXXXXXXX)
trap 'rm -f "$tmp/pig-helpers.jar" "$tmp/classes/nosql/pig/"*.class; rmdir "$tmp/classes/nosql/pig" "$tmp/classes/nosql" "$tmp/classes" "$tmp" 2>/dev/null || true' EXIT
mkdir "$tmp/classes"
hadoop_cp=$("$HADOOP_HOME/bin/hadoop" classpath --glob)
"$JAVA_HOME/bin/javac" --release 11 -encoding UTF-8 \
    -classpath "$PIG_HOME/*:$PIG_HOME/lib/*:$hadoop_cp" \
    -d "$tmp/classes" java/*.java
"$JAVA_HOME/bin/jar" --create --file "$tmp/pig-helpers.jar" -C "$tmp/classes" .
mv "$tmp/pig-helpers.jar" build/pig-helpers.jar
