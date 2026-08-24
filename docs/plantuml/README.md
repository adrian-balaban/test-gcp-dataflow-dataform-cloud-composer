# PlantUML diagrams

PlantUML equivalents of every Mermaid diagram in this repository, plus rendered
`PNG` / `SVG` / `PDF` for each one.

The Mermaid sources remain authoritative — they are what GitHub renders inline.
These are the PlantUML counterparts, useful where Mermaid is not available or
where PlantUML's layout/exports are preferred (print, slides, Confluence).

## Sources

| PlantUML file | Mermaid source |
|---|---|
| `01-pipeline-overview.puml` | `docs/inputs/diagrams/01-pipeline-overview.mmd` |
| `02-engine-dataflow.puml` | `docs/inputs/diagrams/02-engine-dataflow.mmd` |
| `03-demo-sequence.puml` | `docs/inputs/diagrams/03-demo-sequence.mmd` |
| `04-timeline.puml` | `docs/inputs/diagrams/04-timeline.mmd` |
| `readme-01-problem.puml` | `README.md` — Slide 1, The problem |
| `readme-02-pipeline-end-to-end.puml` | `README.md` — Slide 2, The pipeline, end to end |
| `readme-03-c1-system-context.puml` | `README.md` — Slide 3, C1 System Context |
| `readme-04-c2-containers.puml` | `README.md` — Slide 4, C2 Containers |
| `readme-05-two-doors.puml` | `README.md` — Slide 6, Two doors + the balancing equation |
| `readme-06-tds-dat-or-json.puml` | `README.md` — Slide 8, TDS carries `.DAT` or JSON |
| `readme-07-extend-not-rewrite.puml` | `README.md` — Slide 9, Extend, not rewrite |
| `readme-08-one-snapshot-per-run.puml` | `README.md` — Slide 10, One full snapshot per run |
| `readme-09-gcp-architecture.puml` | `README.md` — Slide 11, Real GCP architecture implemented |
| `architecture-01-c3-file-processor.puml` | `ARCHITECTURE.md` — C3 Components (File Processor Pipeline) |
| `architecture-02-c3-recon-service.puml` | `ARCHITECTURE.md` — C3 Components (Recon Service) |
| `architecture-03-c4-code-two-door-engine.puml` | `ARCHITECTURE.md` — C4 Code (the two-door engine core) |

## Rendered documents

Whole Markdown documents rendered to PDF, with **the PlantUML images substituted
in place of the Mermaid blocks** — so the PDF shows the diagrams, not the
un-rendered `mermaid` source.

> Note: `README.pdf` and `ARCHITECTURE.pdf` are the **repository root**
> `README.md` / `ARCHITECTURE.md`. This file (`docs/plantuml/README.md`) is just
> the index for this folder and is not itself exported.

| PDF | Source | Diagrams |
|---|---|---|
| `README.pdf` | `/README.md` | 9 |
| `ARCHITECTURE.pdf` | `/ARCHITECTURE.md` | 3 |
| `PLAN.pdf` | `docs/PLAN.md` | — |
| `PLAN-CHANGES-21082026.pdf` | `docs/PLAN-CHANGES-21082026.md` | — |
| `PLAN-CHANGES-22082026.pdf` | `docs/PLAN-CHANGES-22082026.md` | — |
| `alternative-implementations.pdf` | `docs/alternative-implementations.md` | — |
| `evidence-map.pdf` | `docs/evidence-map.md` | — |
| `production-readiness.pdf` | `docs/production-readiness.md` | — |
| `runbook-gcp.pdf` | `docs/runbook-gcp.md` | — |
| `setup-gcp.pdf` | `docs/setup-gcp.md` | — |

Only `README.md` and `ARCHITECTURE.md` contain Mermaid; the `docs/*.md` files
have none, so those PDFs are straight renderings. Files under `docs/evidence/`
are run evidence, not documentation, and are not exported.

Regenerate with `./render-docs-pdf.py` (needs `pandoc` and `google-chrome`):

```bash
./render-docs-pdf.py
```

The script asserts that the number of Mermaid blocks in each file matches the
number of mapped PlantUML diagrams, so it fails loudly if a diagram is added to
the Markdown without a PlantUML counterpart being registered in `DIAGRAMS`.

## Regenerating the diagram images

`render.sh` needs only a JRE and `plantuml.jar`; the C4 diagrams use the
C4-PlantUML library bundled in PlantUML's stdlib. Every diagram declares
`!pragma layout smetana`, so **Graphviz/`dot` is not required**.

```bash
export PLANTUML_JAR=/path/to/plantuml.jar
./render.sh
```

PDFs are produced from the SVGs with `cairosvg`, because PlantUML's own `-tpdf`
needs Batik/FOP jars that are not part of the standard distribution. If
`cairosvg` is not on `PATH`, `render.sh` renders PNG + SVG and skips the PDFs.

## Notes on the translation

- `01-pipeline-overview.mmd` declares node `Db` but wires up an undefined `Db2`;
  Mermaid silently creates the empty node. The PlantUML version wires the
  labelled `Mainframe Db` node, which is the evident intent.
- Mermaid's `classDef`/`style` colouring is reproduced with inline element
  colours and, for the C4 diagrams, with the standard C4-PlantUML palette.
- Mermaid subgraphs become `rectangle`/`Boundary` containers.
