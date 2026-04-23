# Genealogy Converter

This repository contains a small Python CLI that imports `Genealogi.vdx` into a normalized SQLite genealogy database and exports the same data to GEDCOM, generated VDX, and a static interactive HTML tree.

Published tree:

https://axisrow.github.io/genealogy/

## Contents

- `Genealogi.vdx` - original Visio VDX source.
- `Genealogi.tif` - reference image from the original materials.
- `data/genealogy.sqlite` - imported SQLite database.
- `review.csv` - parser review issues for ambiguous dates, notes, and links.
- `Genealogi.ged` - GEDCOM export.
- `Genealogi.generated.vdx` - regenerated clean VDX tree.
- `site/index.html` - static interactive HTML tree for GitHub Pages.
- `genealogy/` - Python converter package.
- `tests/` - unit tests.

## Usage

Run the commands from the repository root with Python 3.12 or newer.

```bash
python3 -m genealogy.cli import-vdx Genealogi.vdx --db data/genealogy.sqlite --review review.csv
python3 -m genealogy.cli import-media photos.csv --db data/genealogy.sqlite --media-dir media
python3 -m genealogy.cli export-gedcom --db data/genealogy.sqlite --out Genealogi.ged
python3 -m genealogy.cli export-html --db data/genealogy.sqlite --out site/index.html
python3 -m genealogy.cli export-vdx --db data/genealogy.sqlite --out Genealogi.generated.vdx
```

The media manifest CSV format is:

```csv
person_id,name_hint,file,title,notes
P0001,,photo.jpg,Portrait,Album scan
```

`person_id` takes precedence. If it is empty, `name_hint` is used only when it resolves to exactly one person.

## Tests

```bash
python3 -m unittest
python3 -m compileall -q genealogy tests
```

## GitHub Pages

GitHub Actions deploys the static site from `site/` on every push to `main`.
