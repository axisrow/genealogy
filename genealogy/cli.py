from __future__ import annotations

import argparse

from .exporters import export_gedcom, export_html, export_vdx
from .importer import import_media, import_vdx


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="genealogy")
    sub = parser.add_subparsers(dest="command", required=True)

    import_vdx_parser = sub.add_parser("import-vdx", help="Import a Visio VDX org-chart genealogy source")
    import_vdx_parser.add_argument("vdx")
    import_vdx_parser.add_argument("--db", required=True)
    import_vdx_parser.add_argument("--review")

    media_parser = sub.add_parser("import-media", help="Import media manifest CSV")
    media_parser.add_argument("manifest")
    media_parser.add_argument("--db", required=True)
    media_parser.add_argument("--media-dir", required=True)

    gedcom_parser = sub.add_parser("export-gedcom", help="Export GEDCOM")
    gedcom_parser.add_argument("--db", required=True)
    gedcom_parser.add_argument("--out", required=True)

    html_parser = sub.add_parser("export-html", help="Export static HTML")
    html_parser.add_argument("--db", required=True)
    html_parser.add_argument("--out", required=True)

    vdx_parser = sub.add_parser("export-vdx", help="Export generated VDX")
    vdx_parser.add_argument("--db", required=True)
    vdx_parser.add_argument("--out", required=True)

    return parser


def run(argv: list[str] | None = None) -> dict[str, int]:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "import-vdx":
        stats = import_vdx(args.vdx, args.db, args.review)
    elif args.command == "import-media":
        stats = import_media(args.manifest, args.db, args.media_dir)
    elif args.command == "export-gedcom":
        stats = export_gedcom(args.db, args.out)
    elif args.command == "export-html":
        stats = export_html(args.db, args.out)
    elif args.command == "export-vdx":
        stats = export_vdx(args.db, args.out)
    else:
        parser.error(f"unknown command {args.command}")
    return stats


def main(argv: list[str] | None = None) -> int:
    stats = run(argv)
    print(", ".join(f"{key}={value}" for key, value in stats.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
