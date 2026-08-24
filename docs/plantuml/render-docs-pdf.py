#!/usr/bin/env python3
"""Render the repo's Markdown to PDF, substituting PlantUML images for the Mermaid blocks.

Usage:  ./render-docs-pdf.py      (requires pandoc + google-chrome)
Output: docs/plantuml/<NAME>.pdf for README.md, ARCHITECTURE.md and docs/*.md
"""
import os, re, subprocess, sys, tempfile, pathlib, shutil

REPO = pathlib.Path(__file__).resolve().parents[2]
OUT  = REPO / "docs" / "plantuml"

# markdown file -> ordered list of plantuml basenames replacing its mermaid blocks
DIAGRAMS = {
    "README.md": [
        "readme-01-problem", "readme-02-pipeline-end-to-end",
        "readme-03-c1-system-context", "readme-04-c2-containers",
        "readme-05-two-doors", "readme-06-tds-dat-or-json",
        "readme-07-extend-not-rewrite", "readme-08-one-snapshot-per-run",
        "readme-09-gcp-architecture",
    ],
    "ARCHITECTURE.md": [
        "architecture-01-c3-file-processor",
        "architecture-02-c3-recon-service",
        "architecture-03-c4-code-two-door-engine",
    ],
}

CSS = """
@page { size: A4; margin: 16mm 14mm; }
* { box-sizing: border-box; }
body { font-family: -apple-system,'Segoe UI',Helvetica,Arial,sans-serif;
       font-size: 10.5pt; line-height: 1.55; color: #1b1b1b; margin: 0; }
h1,h2,h3,h4 { line-height: 1.25; margin: 1.4em 0 .5em; page-break-after: avoid; }
h1 { font-size: 20pt; border-bottom: 2px solid #ddd; padding-bottom: .25em; }
h2 { font-size: 15pt; border-bottom: 1px solid #eee; padding-bottom: .2em; }
h3 { font-size: 12.5pt; } h4 { font-size: 11pt; }
p, li { orphans: 2; widows: 2; }
code { font-family: 'DejaVu Sans Mono',Menlo,Consolas,monospace; font-size: 8.8pt;
       background: #f3f4f6; padding: .12em .35em; border-radius: 3px; }
pre { background: #f6f8fa; border: 1px solid #e2e5e9; border-radius: 5px;
      padding: 9px 11px; overflow: hidden; page-break-inside: avoid; }
pre code { background: none; padding: 0; font-size: 8.2pt; white-space: pre-wrap;
           word-wrap: break-word; }
table { border-collapse: collapse; width: 100%; margin: .9em 0; font-size: 8.8pt;
        page-break-inside: avoid; }
th, td { border: 1px solid #d6d9dd; padding: 5px 8px; text-align: left;
         vertical-align: top; }
th { background: #f3f4f6; font-weight: 600; }
blockquote { margin: .9em 0; padding: .1em 1em; border-left: 4px solid #d6d9dd; color: #444; }
img { max-width: 100%; height: auto; }
hr { border: none; border-top: 1px solid #e2e5e9; margin: 1.6em 0; }
a { color: #0b5fbf; text-decoration: none; word-break: break-word; }
figure.diagram { margin: 1.1em 0; text-align: center; page-break-inside: avoid; }
figure.diagram img { max-width: 100%; max-height: 232mm; }
figure.diagram figcaption { font-size: 8pt; color: #777; margin-top: .45em; font-style: italic; }
"""

def substitute(md_text, names, src_name):
    """Replace each ```mermaid fence with an <img> to the matching PlantUML SVG."""
    out, i, n = [], 0, 0
    lines = md_text.split("\n")
    while i < len(lines):
        if lines[i].strip().startswith("```mermaid"):
            j = i + 1
            while j < len(lines) and lines[j].strip() != "```":
                j += 1
            if n < len(names):
                svg = OUT / (names[n] + ".svg")
                if not svg.exists():
                    raise SystemExit(f"missing render: {svg}")
                out.append("")
                out.append(f'<figure class="diagram">'
                           f'<img src="file://{svg}" alt="{names[n]}">'
                           f'<figcaption>{names[n]}.puml</figcaption></figure>')
                out.append("")
            else:
                raise SystemExit(f"{src_name}: more mermaid blocks than mapped diagrams")
            n += 1
            i = j + 1
            continue
        out.append(lines[i])
        i += 1
    if n != len(names):
        raise SystemExit(f"{src_name}: expected {len(names)} mermaid blocks, found {n}")
    return "\n".join(out), n

def main():
    targets = [REPO / "README.md", REPO / "ARCHITECTURE.md"]
    targets += sorted((REPO / "docs").glob("*.md"))

    tmp = pathlib.Path(tempfile.mkdtemp(prefix="md2pdf-"))
    css = tmp / "s.css"; css.write_text(CSS)
    ok = 0
    for md in targets:
        rel = md.relative_to(REPO).as_posix()
        names = DIAGRAMS.get(rel, [])
        text = md.read_text()
        text, n = substitute(text, names, rel)

        stem = "ARCHITECTURE" if rel == "ARCHITECTURE.md" else (
               "README" if rel == "README.md" else md.stem)
        html = tmp / (stem + ".html")
        pdf  = OUT / (stem + ".pdf")

        src = tmp / (stem + ".md"); src.write_text(text)
        subprocess.run(["pandoc", "-f", "gfm", "-t", "html5", "--standalone",
                        "--metadata", f"title={stem}", "--css", str(css),
                        "-o", str(html), str(src)], check=True)
        subprocess.run(["google-chrome", "--headless=new", "--disable-gpu",
                        "--no-sandbox", "--no-pdf-header-footer",
                        "--virtual-time-budget=20000",
                        f"--print-to-pdf={pdf}", f"file://{html}"],
                       check=True, capture_output=True)
        size = pdf.stat().st_size
        print(f"  {stem+'.pdf':<34} {size/1024:7.1f} KB   ({n} diagram(s))")
        ok += 1
    shutil.rmtree(tmp, ignore_errors=True)
    print(f"\n{ok} PDFs written to {OUT}")

main()
