# Repository Guidelines

## Project Structure & Module Organization

`genealogy/` contains the CLI and conversion pipeline: `cli.py` defines subcommands, `vdx.py` parses Visio VDX input, `importer.py` normalizes records into SQLite, `db.py` owns the schema, and `exporters.py` writes GEDCOM, HTML, and regenerated VDX output. `tests/` holds unit tests. Repository-root artifacts such as `Genealogi.vdx`, `Genealogi.ged`, `Genealogi.generated.vdx`, `review.csv`, and `site/index.html` are generated or source data files, not package code. Persistent SQLite data lives in `data/genealogy.sqlite`.

## Build, Test, and Development Commands

Use Python 3.12+ from the repository root.

```bash
python3 -m unittest
python3 -m compileall -q genealogy tests
python3 -m genealogy.cli import-vdx Genealogi.vdx --db data/genealogy.sqlite --review review.csv
python3 -m genealogy.cli export-html --db data/genealogy.sqlite --out site/index.html
```

`python3 -m unittest` runs the current test suite. `python3 -m compileall -q genealogy tests` catches syntax issues without extra tooling. The CLI commands rebuild the database and exports; run them after parser or exporter changes.

## Coding Style & Naming Conventions

Follow standard Python style with 4-space indentation, `snake_case` for functions and variables, and `UPPER_CASE` for module constants. Keep modules dependency-free; this project currently uses only the standard library. Prefer small, explicit helper functions and straightforward SQL over framework abstractions. When adding CLI commands, match the existing hyphenated subcommand pattern such as `import-vdx` and `export-gedcom`.

## Testing Guidelines

Add tests under `tests/` using `unittest.TestCase`. Name new files `test_*.py` and test methods `test_*`. Cover both data import behavior and output generation when changing parsing or export code. For pipeline-sensitive work, verify the exact emitted content where practical, as existing tests do for GEDCOM, HTML, and VDX output.

## Commit & Pull Request Guidelines

Recent commits use short imperative subjects such as `Render genealogy as interactive family tree` and `Publish genealogy tree`. Keep commit titles concise, descriptive, and action-oriented. PRs should explain the user-visible effect, note any regenerated artifacts committed with the change, and link the relevant issue if one exists. Include screenshots only when changing `site/index.html` rendering or tree layout behavior.

## Data & Output Notes

Treat source genealogy files as sensitive historical data. Avoid unnecessary edits to checked-in exports unless your change intentionally regenerates them. If output files change, mention which command produced them in the PR description.
