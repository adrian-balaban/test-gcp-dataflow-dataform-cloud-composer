#!/usr/bin/env bash
# Regenerate PNG / SVG / PDF for every .puml in this directory.
#
#   PLANTUML_JAR=/path/to/plantuml.jar ./render.sh
#
# Requires: a JRE. Graphviz is NOT required (the diagrams use `!pragma layout smetana`).
# PDFs additionally require `cairosvg`; if it is missing, PDFs are skipped.
set -euo pipefail

cd "$(dirname "$0")"

JAR="${PLANTUML_JAR:-plantuml.jar}"
if [[ ! -f "$JAR" ]]; then
    echo "plantuml.jar not found at '$JAR'." >&2
    echo "Download it from https://plantuml.com/download and set PLANTUML_JAR." >&2
    exit 1
fi

echo "rendering SVG..."
java -Djava.awt.headless=true -jar "$JAR" -tsvg ./*.puml

echo "rendering PNG..."
java -Djava.awt.headless=true -jar "$JAR" -tpng ./*.puml

if command -v cairosvg >/dev/null 2>&1; then
    echo "rendering PDF..."
    for svg in ./*.svg; do
        cairosvg "$svg" -o "${svg%.svg}.pdf"
    done
else
    echo "cairosvg not found — skipping PDF generation." >&2
fi

echo "done"
