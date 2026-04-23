from __future__ import annotations

import csv
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .db import connect, init_db
from .vdx import VdxData, parse_vdx_file


DATE_RE = r"(\d{1,2}[./]\d{1,2}[./]\d{2,4}|\d{4})"
NOTE_PAREN_RE = re.compile(r"\([^)]*(?:л\.?|l\.?|sheet|archive|архив|об)[^)]*\)", re.IGNORECASE)
EVENT_PATTERNS = {
    "birth": re.compile(rf"\b(?:р\.|род\.|b\.)\s*{DATE_RE}\s*(?:г\.?)?", re.IGNORECASE),
    "christening": re.compile(rf"\b(?:кр\.?|kr\.?)\s*{DATE_RE}\s*(?:г\.?)?", re.IGNORECASE),
    "death": re.compile(rf"\b(?:умер|died)\s*{DATE_RE}\s*(?:г\.?)?", re.IGNORECASE),
}


@dataclass
class ParsedPerson:
    display_name: str
    birth: str | None = None
    christening: str | None = None
    death: str | None = None
    notes: str = ""
    issue_types: tuple[str, ...] = ()


def parse_person_fragment(text: str) -> ParsedPerson:
    original = " ".join(text.split())
    working = original
    notes: list[str] = []

    for match in NOTE_PAREN_RE.findall(working):
        notes.append(match)
        working = working.replace(match, " ")

    archive_match = re.search(r"\b(?:archive|архив)\b.*$", working, re.IGNORECASE)
    if archive_match:
        notes.append(archive_match.group(0).strip())
        working = working[: archive_match.start()]

    events: dict[str, str | None] = {"birth": None, "christening": None, "death": None}
    for event, pattern in EVENT_PATTERNS.items():
        match = pattern.search(working)
        if match:
            events[event] = match.group(1)
            working = pattern.sub(" ", working, count=1)

    words = [part for part in re.split(r"\s+", working.strip(" ,;")) if part]
    display_parts: list[str] = []
    trailing_notes: list[str] = []
    for part in words:
        if re.fullmatch(DATE_RE, part) or part.lower() in {"г.", "г"}:
            trailing_notes.append(part)
            continue
        if trailing_notes:
            trailing_notes.append(part)
        else:
            display_parts.append(part)

    display_name = " ".join(display_parts).strip(" ,;") or original
    if trailing_notes:
        notes.append(" ".join(trailing_notes))

    issue_types: list[str] = []
    if not events["birth"] and not events["christening"] and not events["death"] and re.search(DATE_RE, original):
        issue_types.append("ambiguous_date")
    if notes:
        issue_types.append("note_fragment")
    if display_name == original and ("+" in original or re.search(DATE_RE, original)):
        issue_types.append("low_confidence_parse")

    return ParsedPerson(
        display_name=display_name,
        birth=events["birth"],
        christening=events["christening"],
        death=events["death"],
        notes="; ".join(notes),
        issue_types=tuple(dict.fromkeys(issue_types)),
    )


def split_position_name(name: str) -> list[str]:
    parts = [part.strip() for part in name.split("+")]
    return [part for part in parts if part]


def _next_id(prefix: str, number: int) -> str:
    return f"{prefix}{number:04d}"


def _name_parts(display_name: str) -> tuple[str, str]:
    pieces = display_name.split()
    if len(pieces) <= 1:
        return display_name, ""
    return " ".join(pieces[:-1]), pieces[-1]


def import_vdx(vdx_path: str | Path, db_path: str | Path, review_path: str | Path | None = None) -> dict[str, int]:
    data = parse_vdx_file(vdx_path)
    conn = connect(db_path)
    init_db(conn, reset=True)
    try:
        stats = import_vdx_data(conn, data)
        conn.commit()
        if review_path:
            write_review_csv(conn, review_path)
        return stats
    finally:
        conn.close()


def import_vdx_data(conn: sqlite3.Connection, data: VdxData) -> dict[str, int]:
    person_num = 1
    family_num = 1
    source_to_primary: dict[str, str] = {}
    source_to_family: dict[str, str] = {}

    for position in data.positions:
        fragments = split_position_name(position.name)
        person_ids: list[str] = []
        for fragment in fragments:
            parsed = parse_person_fragment(fragment)
            person_id = _next_id("P", person_num)
            person_num += 1
            given, surname = _name_parts(parsed.display_name)
            conn.execute(
                """
                insert into persons (
                    person_id, source_visio_id, display_name, given_name, surname,
                    original_text, birth, christening, death, notes
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    person_id,
                    position.source_id,
                    parsed.display_name,
                    given,
                    surname,
                    fragment,
                    parsed.birth,
                    parsed.christening,
                    parsed.death,
                    parsed.notes,
                ),
            )
            person_ids.append(person_id)
            for issue in parsed.issue_types:
                add_issue(conn, position.source_id, person_id, issue, f"Review parsed fragment for {parsed.display_name}", fragment)

        if not person_ids:
            add_issue(conn, position.source_id, None, "unsplit_name", "Position did not contain a usable name", position.name)
            continue

        family_id = None
        if len(person_ids) >= 2:
            family_id = _next_id("F", family_num)
            family_num += 1
            conn.execute(
                """
                insert into families (
                    family_id, spouse1_person_id, spouse2_person_id, source_visio_id,
                    original_combined_text, confidence, review_status
                ) values (?, ?, ?, ?, ?, ?, ?)
                """,
                (family_id, person_ids[0], person_ids[1], position.source_id, position.name, "medium", "review"),
            )
            if len(person_ids) > 2:
                add_issue(conn, position.source_id, person_ids[0], "ambiguous_split", "More than two plus-separated fragments", position.name)

        source_to_primary[position.source_id] = person_ids[0]
        if family_id:
            source_to_family[position.source_id] = family_id
        conn.execute(
            "insert into source_nodes (source_visio_id, primary_person_id, family_id, original_text) values (?, ?, ?, ?)",
            (position.source_id, person_ids[0], family_id, position.name),
        )

    outgoing_sources = {report.source_id for report in data.reports}
    for source_id in sorted(outgoing_sources):
        if source_id in source_to_primary and source_id not in source_to_family:
            family_id = _next_id("F", family_num)
            family_num += 1
            source_to_family[source_id] = family_id
            original_text = conn.execute(
                "select original_text from source_nodes where source_visio_id = ?", (source_id,)
            ).fetchone()["original_text"]
            conn.execute(
                """
                insert into families (
                    family_id, spouse1_person_id, source_visio_id, original_combined_text,
                    confidence, review_status
                ) values (?, ?, ?, ?, ?, ?)
                """,
                (family_id, source_to_primary[source_id], source_id, original_text, "low", "review"),
            )
            conn.execute("update source_nodes set family_id = ? where source_visio_id = ?", (family_id, source_id))

    for report in data.reports:
        family_id = source_to_family.get(report.source_id)
        child_id = source_to_primary.get(report.target_id)
        if not family_id:
            add_issue(conn, report.source_id, None, "orphaned_link", "ReportsTo source has no imported family/person", str(report))
            continue
        if not child_id:
            add_issue(conn, report.target_id, None, "missing_target", "ReportsTo target has no imported person", str(report))
            continue
        conn.execute(
            "insert or ignore into family_children (family_id, child_person_id, source_visio_id) values (?, ?, ?)",
            (family_id, child_id, report.source_id),
        )

    return {
        "positions": len(data.positions),
        "reports": len(data.reports),
        "persons": conn.execute("select count(*) from persons").fetchone()[0],
        "families": conn.execute("select count(*) from families").fetchone()[0],
        "relationships": conn.execute("select count(*) from family_children").fetchone()[0],
        "issues": conn.execute("select count(*) from import_issues").fetchone()[0],
    }


def add_issue(
    conn: sqlite3.Connection,
    source_visio_id: str | None,
    person_id: str | None,
    issue_type: str,
    message: str,
    raw_text: str,
) -> None:
    conn.execute(
        "insert into import_issues (source_visio_id, person_id, issue_type, message, raw_text) values (?, ?, ?, ?, ?)",
        (source_visio_id, person_id, issue_type, message, raw_text),
    )


def write_review_csv(conn: sqlite3.Connection, path: str | Path) -> None:
    review_path = Path(path)
    if review_path.parent and str(review_path.parent) != ".":
        review_path.parent.mkdir(parents=True, exist_ok=True)
    rows = conn.execute(
        "select source_visio_id, person_id, issue_type, message, raw_text from import_issues order by issue_id"
    ).fetchall()
    with review_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["source_visio_id", "person_id", "issue_type", "message", "raw_text"])
        writer.writerows([tuple(row) for row in rows])


def import_media(manifest_path: str | Path, db_path: str | Path, media_dir: str | Path) -> dict[str, int]:
    conn = connect(db_path)
    init_db(conn, reset=False)
    imported = 0
    issues = 0
    media_prefix = Path(media_dir)
    try:
        with Path(manifest_path).open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                person_id = (row.get("person_id") or "").strip()
                name_hint = (row.get("name_hint") or "").strip()
                file_name = (row.get("file") or "").strip()
                if not file_name:
                    issues += 1
                    add_issue(conn, None, person_id or None, "media_missing_file", "Media row has no file value", str(row))
                    continue
                if not person_id and name_hint:
                    matches = conn.execute(
                        "select person_id from persons where lower(display_name) = lower(?)", (name_hint,)
                    ).fetchall()
                    if len(matches) == 1:
                        person_id = matches[0]["person_id"]
                    else:
                        issues += 1
                        add_issue(conn, None, None, "media_name_not_unique", "name_hint did not resolve uniquely", str(row))
                        continue
                exists = conn.execute("select 1 from persons where person_id = ?", (person_id,)).fetchone()
                if not exists:
                    issues += 1
                    add_issue(conn, None, person_id or None, "media_missing_person", "Media person_id not found", str(row))
                    continue
                relative_path = str(media_prefix / file_name)
                conn.execute(
                    "insert into media (person_id, relative_file_path, title, media_type, notes) values (?, ?, ?, ?, ?)",
                    (
                        person_id,
                        relative_path,
                        (row.get("title") or "").strip(),
                        _media_type(file_name),
                        (row.get("notes") or "").strip(),
                    ),
                )
                imported += 1
        conn.commit()
        return {"media": imported, "issues": issues}
    finally:
        conn.close()


def _media_type(path: str) -> str:
    suffix = Path(path).suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".tif", ".tiff"}:
        return "image"
    return "file"
