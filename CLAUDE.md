# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Python CLI that converts a Visio VDX genealogy org-chart into a normalized SQLite database and exports to GEDCOM 5.5.1, regenerated VDX, and a static interactive SVG/HTML family tree published on GitHub Pages.

Requires Python >= 3.12. No external dependencies — only the standard library.

## Commands

```bash
# Run all tests
python3 -m unittest

# Check syntax across the package
python3 -m compileall -q genealogy tests

# Full pipeline (from repo root)
python3 -m genealogy.cli import-vdx Genealogi.vdx --db data/genealogy.sqlite --review review.csv
python3 -m genealogy.cli import-media photos.csv --db data/genealogy.sqlite --media-dir media
python3 -m genealogy.cli export-gedcom --db data/genealogy.sqlite --out Genealogi.ged
python3 -m genealogy.cli export-html --db data/genealogy.sqlite --out site/index.html
python3 -m genealogy.cli export-vdx --db data/genealogy.sqlite --out Genealogi.generated.vdx
```

## Architecture

**Data flow:** VDX file → SQLite (normalized) → GEDCOM / HTML / VDX exports

### `genealogy/vdx.py` — VDX parser

Parses Visio XML using regex (not an XML parser). Extracts `VdxPosition` (org-chart nodes with Name/Title text) and `VdxReport` (ReportsTo links between nodes). The `Position`/`ReportsTo` elements use the `mstns:` namespace.

### `genealogy/importer.py` — VDX import logic

The core parsing intelligence. Splits `+`-separated names (spouse pairs), extracts birth/christening/death dates using regex patterns (`р.`, `кр.`, `умер` and English equivalents), and pulls archive/sheet notes from parenthetical fragments. Creates persons, families, and parent-child relationships in SQLite. Ambiguous parses are logged to `import_issues` and written to `review.csv`.

ID generation: persons get `P0001`, `P0002`...; families get `F0001`, `F0002`... — sequential within an import run.

### `genealogy/db.py` — SQLite schema

Tables: `persons`, `families`, `family_children`, `source_nodes`, `media`, `import_issues`. Foreign keys enforced. `source_visio_id` columns preserve the link back to the original VDX node for traceability.

### `genealogy/exporters.py` — Three export formats

- **GEDCOM** (`export_gedcom`): Standard lineage-linked GEDCOM 5.5.1 with media references.
- **HTML** (`export_html`): Single-file interactive SVG family tree using a **focus-mode** UX: the canvas shows one scene at a time around a selected person (up to 3 ancestor generations, siblings, spouses, children, and chips for deeper descendants). Python ships the full person/family graph as JSON; the client (`computeScene`/`layoutScene`/`drawScene` in the embedded JS) computes a small per-focus layout on demand. Clicking any card or sidebar entry re-focuses the scene; URL hash (`#P0042`) and `localStorage` persist state. Canonical "clan roots" are found via union-find and chosen by largest descendant subtree.
- **VDX** (`export_vdx`): Regenerated Visio XML with simple BFS-leveled layout.

### `genealogy/cli.py` — CLI entry point

`argparse`-based subcommands: `import-vdx`, `import-media`, `export-gedcom`, `export-html`, `export-vdx`. Each command returns a stats dict printed on completion.

## Key Design Decisions

- The VDX parser uses regex because the input is a specific Visio org-chart format with consistent structure, not arbitrary XML.
- The HTML tree is a single self-contained file (no build step, no JS framework) — the layout is pre-computed in Python and serialized as JSON into the HTML template.
- `import-media` resolves person references by `person_id` first, falling back to `name_hint` (unique name match).

## GitHub Pages

`.github/workflows/pages.yml` deploys `site/` on every push to `main`. The published tree is at https://axisrow.github.io/genealogy/
