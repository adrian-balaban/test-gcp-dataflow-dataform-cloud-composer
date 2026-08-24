#!/usr/bin/env bash
# Regenerate the MVP plan diagrams from their Mermaid sources.
# Requires mermaid-cli:  npm i -g @mermaid-js/mermaid-cli
set -euo pipefail

cd "$(dirname "$0")"

for src in *.mmd; do
    out="${src%.mmd}.png"
    echo "rendering $src -> $out"
    mmdc -i "$src" -o "$out" -b white -s 3
done

echo "done"
