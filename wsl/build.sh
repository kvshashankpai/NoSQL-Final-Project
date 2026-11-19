# Rebuild both helper JARs (only needed after editing the Java sources).
set -euo pipefail
source "$(dirname "$0")/env.sh"
bash "$NOSQL_HOME/pipelines/pig/build.sh"
cd "$NOSQL_HOME/pipelines/mapreduce"
tmp=$(mktemp -d)
javac --release 11 -encoding UTF-8 -nowarn -cp "$(hadoop classpath)" -d "$tmp" $(find src -name '*.java')
mkdir -p build && jar cf build/mapreduce-parser.jar -C "$tmp" .
rm -rf "$tmp"
echo "built pipelines/pig/build/pig-helpers.jar and pipelines/mapreduce/build/mapreduce-parser.jar"
