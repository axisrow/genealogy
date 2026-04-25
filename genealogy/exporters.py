from __future__ import annotations

import json
import sqlite3
from collections import defaultdict, deque
from pathlib import Path
import re
from xml.sax.saxutils import escape

from .db import connect


GEDCOM_LINE_LIMIT = 200
GEDCOM_MONTHS = {
    "01": "JAN",
    "02": "FEB",
    "03": "MAR",
    "04": "APR",
    "05": "MAY",
    "06": "JUN",
    "07": "JUL",
    "08": "AUG",
    "09": "SEP",
    "10": "OCT",
    "11": "NOV",
    "12": "DEC",
}


def export_gedcom(db_path: str | Path, out_path: str | Path) -> dict[str, int]:
    conn = connect(db_path)
    try:
        persons = conn.execute("select * from persons order by person_id").fetchall()
        families = conn.execute("select * from families order by family_id").fetchall()
        children = conn.execute("select * from family_children order by family_id, child_person_id").fetchall()
        media = conn.execute("select * from media order by media_id").fetchall()
    finally:
        conn.close()

    media_by_person = defaultdict(list)
    for item in media:
        media_by_person[item["person_id"]].append(item)
    children_by_family = defaultdict(list)
    for child in children:
        children_by_family[child["family_id"]].append(child["child_person_id"])

    lines = [
        "0 HEAD",
        "1 SOUR genealogy-converter",
        "1 GEDC",
        "2 VERS 5.5.1",
        "2 FORM LINEAGE-LINKED",
        "1 CHAR UTF-8",
        "1 SUBM @SUB1@",
    ]
    for person in persons:
        lines.append(f"0 @{person['person_id']}@ INDI")
        _emit_gedcom_value(lines, 1, "NAME", person["display_name"])
        if person["birth"]:
            lines.extend(["1 BIRT", f"2 DATE {_format_gedcom_date(person['birth'])}"])
        if person["christening"]:
            lines.extend(["1 CHR", f"2 DATE {_format_gedcom_date(person['christening'])}"])
        if person["death"]:
            lines.extend(["1 DEAT", f"2 DATE {_format_gedcom_date(person['death'])}"])
        if person["notes"]:
            _emit_gedcom_value(lines, 1, "NOTE", person["notes"])
        for item in media_by_person[person["person_id"]]:
            lines.append("1 OBJE")
            _emit_gedcom_value(lines, 2, "FILE", item["relative_file_path"])
            if item["title"]:
                _emit_gedcom_value(lines, 2, "TITL", item["title"])

    for family in families:
        lines.append(f"0 @{family['family_id']}@ FAM")
        if family["spouse1_person_id"]:
            lines.append(f"1 HUSB @{family['spouse1_person_id']}@")
        if family["spouse2_person_id"]:
            lines.append(f"1 WIFE @{family['spouse2_person_id']}@")
        for child_id in children_by_family[family["family_id"]]:
            lines.append(f"1 CHIL @{child_id}@")
        if family["original_combined_text"]:
            _emit_gedcom_value(lines, 1, "NOTE", f"Source text: {family['original_combined_text']}")
    lines.extend(["0 @SUB1@ SUBM", "1 NAME genealogy-converter"])
    lines.append("0 TRLR")

    _write_text(out_path, "\n".join(lines) + "\n")
    return {"persons": len(persons), "families": len(families)}


def export_html(db_path: str | Path, out_path: str | Path) -> dict[str, int]:
    conn = connect(db_path)
    try:
        persons = [dict(row) for row in conn.execute("select * from persons order by person_id")]
        families = [dict(row) for row in conn.execute("select * from families order by family_id")]
        children = [dict(row) for row in conn.execute("select * from family_children order by family_id, child_person_id")]
        media = [dict(row) for row in conn.execute("select * from media order by media_id")]
    finally:
        conn.close()

    payload = _build_html_payload(persons, families, children, media)
    data_json = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    html = HTML_TEMPLATE.replace("__DATA__", data_json)
    _write_text(out_path, html)
    return {"persons": len(persons), "families": len(families)}


def export_vdx(db_path: str | Path, out_path: str | Path) -> dict[str, int]:
    conn = connect(db_path)
    try:
        persons = [dict(row) for row in conn.execute("select * from persons order by person_id")]
        families = [dict(row) for row in conn.execute("select * from families order by family_id")]
        children = [dict(row) for row in conn.execute("select * from family_children order by family_id, child_person_id")]
    finally:
        conn.close()

    layout = _layout_nodes(persons, families, children)
    lines = [
        "<?xml version='1.0' encoding='utf-8'?>",
        "<VisioDocument xmlns='http://schemas.microsoft.com/visio/2003/core'>",
        "  <Pages><Page ID='1' Name='Genealogy'><Shapes>",
    ]
    shape_id = 1
    node_to_shape: dict[str, int] = {}
    for node_id, node in layout.items():
        node_to_shape[node_id] = shape_id
        text = escape(node["label"])
        lines.extend(
            [
                f"    <Shape ID='{shape_id}' NameU='Process' Type='Shape'>",
                f"      <XForm><PinX>{node['x']:.2f}</PinX><PinY>{node['y']:.2f}</PinY><Width>1.8</Width><Height>0.55</Height></XForm>",
                f"      <Text>{text}</Text>",
                "    </Shape>",
            ]
        )
        shape_id += 1

    for child in children:
        family_node = f"family:{child['family_id']}"
        child_node = f"person:{child['child_person_id']}"
        if family_node not in node_to_shape or child_node not in node_to_shape:
            continue
        lines.extend(
            [
                f"    <Shape ID='{shape_id}' NameU='Dynamic connector' Type='Shape'>",
                f"      <Connect FromSheet='{shape_id}' FromCell='BeginX' ToSheet='{node_to_shape[family_node]}' ToCell='PinX'/>",
                f"      <Connect FromSheet='{shape_id}' FromCell='EndX' ToSheet='{node_to_shape[child_node]}' ToCell='PinX'/>",
                "    </Shape>",
            ]
        )
        shape_id += 1

    lines.extend(["  </Shapes></Page></Pages>", "</VisioDocument>"])
    _write_text(out_path, "\n".join(lines) + "\n")
    return {"shapes": len(layout), "connectors": len(children)}


def _layout_nodes(persons: list[dict], families: list[dict], children: list[dict]) -> dict[str, dict]:
    person_by_id = {person["person_id"]: person for person in persons}
    family_by_id = {family["family_id"]: family for family in families}
    children_by_family = defaultdict(list)
    parent_family_by_child = {}
    for row in children:
        children_by_family[row["family_id"]].append(row["child_person_id"])
        parent_family_by_child.setdefault(row["child_person_id"], row["family_id"])

    roots = [family["family_id"] for family in families if family["spouse1_person_id"] not in parent_family_by_child]
    if not roots:
        roots = [family["family_id"] for family in families]

    levels: dict[str, int] = {}
    queue = deque([(f"family:{family_id}", 0) for family_id in roots])
    while queue:
        node_id, level = queue.popleft()
        if node_id in levels and levels[node_id] <= level:
            continue
        levels[node_id] = level
        if node_id.startswith("family:"):
            family_id = node_id.split(":", 1)[1]
            family = family_by_id.get(family_id)
            if family:
                for spouse_key in ("spouse1_person_id", "spouse2_person_id"):
                    if family.get(spouse_key):
                        levels.setdefault(f"person:{family[spouse_key]}", level)
            for child_id in children_by_family.get(family_id, []):
                queue.append((f"person:{child_id}", level + 1))
        elif node_id.startswith("person:"):
            person_id = node_id.split(":", 1)[1]
            for family in families:
                if family["spouse1_person_id"] == person_id or family["spouse2_person_id"] == person_id:
                    queue.append((f"family:{family['family_id']}", level))

    for person in persons:
        levels.setdefault(f"person:{person['person_id']}", 0)
    for family in families:
        levels.setdefault(f"family:{family['family_id']}", 0)

    buckets = defaultdict(list)
    for node_id, level in levels.items():
        buckets[level].append(node_id)

    layout: dict[str, dict] = {}
    for level, node_ids in buckets.items():
        for index, node_id in enumerate(sorted(node_ids)):
            if node_id.startswith("person:"):
                person = person_by_id[node_id.split(":", 1)[1]]
                label = person["display_name"]
            else:
                family = family_by_id[node_id.split(":", 1)[1]]
                labels = [
                    person_by_id[pid]["display_name"]
                    for pid in (family["spouse1_person_id"], family["spouse2_person_id"])
                    if pid and pid in person_by_id
                ]
                label = " + ".join(labels) or family["family_id"]
            layout[node_id] = {"x": 1.5 + index * 2.4, "y": 10.0 - level * 1.4, "label": label}
    return layout


def _write_text(path: str | Path, text: str) -> None:
    out_path = Path(path)
    if out_path.parent and str(out_path.parent) != ".":
        out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")


def _escape_gedcom_value(value: object) -> str:
    return str(value).replace("@", "@@")


def _split_gedcom_line(value: str, limit: int = GEDCOM_LINE_LIMIT) -> list[str]:
    if len(value) <= limit:
        return [value]
    chunks = []
    remaining = value
    while len(remaining) > limit:
        split_at = remaining.rfind(" ", 0, limit + 1)
        if split_at <= 0:
            split_at = limit
        chunks.append(remaining[:split_at])
        remaining = remaining[split_at:].lstrip()
    chunks.append(remaining)
    return chunks


def _emit_gedcom_value(lines: list[str], level: int, tag: str, value: object) -> None:
    normalized = _escape_gedcom_value(value).replace("\r\n", "\n").replace("\r", "\n")
    logical_lines = normalized.split("\n") or [""]
    for index, logical_line in enumerate(logical_lines):
        chunks = _split_gedcom_line(logical_line)
        first_tag = tag if index == 0 else "CONT"
        first_level = level if index == 0 else level + 1
        lines.append(f"{first_level} {first_tag} {chunks[0]}")
        for chunk in chunks[1:]:
            lines.append(f"{level + 1} CONC {chunk}")


def _format_gedcom_date(value: str) -> str:
    stripped = value.strip()
    match = re.fullmatch(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", stripped)
    if not match:
        return _escape_gedcom_value(stripped)
    day, month, year = match.groups()
    month_name = GEDCOM_MONTHS.get(month.zfill(2))
    if not month_name:
        return _escape_gedcom_value(stripped)
    return f"{int(day)} {month_name} {year}"


def _build_html_payload(persons: list[dict], families: list[dict], children: list[dict], media: list[dict]) -> dict:
    person_by_id = {person["person_id"]: person for person in persons}
    children_by_family: dict[str, list[str]] = defaultdict(list)
    spouse_families_by_person: dict[str, list[str]] = defaultdict(list)
    parent_family_by_person: dict[str, str] = {}
    media_by_person: dict[str, list[dict]] = defaultdict(list)

    for item in media:
        media_by_person[item["person_id"]].append(dict(item))
    for family in families:
        for person_id in (family["spouse1_person_id"], family["spouse2_person_id"]):
            if person_id:
                spouse_families_by_person[person_id].append(family["family_id"])
    for link in children:
        children_by_family[link["family_id"]].append(link["child_person_id"])
        parent_family_by_person.setdefault(link["child_person_id"], link["family_id"])

    person_payload = []
    for person in persons:
        pid = person["person_id"]
        media_list = media_by_person.get(pid, [])
        avatar = None
        for m in media_list:
            path = m.get("relative_file_path") or ""
            if path and (m.get("media_type") in (None, "image", "photo", "portrait") or path.lower().endswith((".jpg", ".jpeg", ".png", ".webp", ".gif"))):
                avatar = path
                break
        person_payload.append(
            {
                "person_id": pid,
                "display_name": person["display_name"],
                "name_lines": _wrap_label(person["display_name"], 18, 2),
                "short_name": _short_name(person["display_name"]),
                "initials": _initials(person["display_name"]),
                "years": _short_year_label(person),
                "birth": person.get("birth"),
                "christening": person.get("christening"),
                "death": person.get("death"),
                "notes": person.get("notes"),
                "source_visio_id": person.get("source_visio_id"),
                "avatar": avatar,
                "media": [
                    {"title": m.get("title"), "relative_file_path": m.get("relative_file_path")}
                    for m in media_list
                ],
            }
        )

    family_payload = []
    for family in families:
        spouse_ids = [family["spouse1_person_id"], family["spouse2_person_id"]]
        family_payload.append(
            {
                "family_id": family["family_id"],
                "spouses": [sid for sid in spouse_ids if sid and sid in person_by_id],
                "children": list(children_by_family.get(family["family_id"], [])),
            }
        )

    root_persons, person_roots = _compute_root_persons(
        person_by_id, parent_family_by_person, families, spouse_families_by_person, children_by_family
    )
    default_focus = _select_default_focus(
        person_by_id, children_by_family, spouse_families_by_person, parent_family_by_person, root_persons
    )
    overview = _build_overview_layout(
        family_payload, parent_family_by_person, spouse_families_by_person, root_persons, person_roots
    )

    return {
        "persons": person_payload,
        "families": family_payload,
        "parentFamily": parent_family_by_person,
        "spouseFamilies": {pid: fids for pid, fids in spouse_families_by_person.items()},
        "rootPersons": root_persons,
        "personRoots": person_roots,
        "defaultFocus": default_focus,
        "overview": overview,
    }


# ---- Overview layout (server-side canvas of the whole tree) ----

OVERVIEW_CARD_W = 150
OVERVIEW_CARD_H = 60
OVERVIEW_GEN_GAP = 90
OVERVIEW_SIB_GAP = 18
OVERVIEW_SPOUSE_GAP = 14
OVERVIEW_BRANCH_GAP = 60
OVERVIEW_PADDING = 40
OVERVIEW_MAX_CHILDREN_PER_ROW = 6


def _build_overview_layout(
    family_payload: list[dict],
    parent_family_by_person: dict[str, str],
    spouse_families_by_person: dict[str, list[str]],
    root_persons: list[str],
    person_roots: dict[str, str],
) -> dict:
    """Simple top-down tiling layout: each clan becomes a block; within a block,
    descendants are placed as a left-to-right generational tree under the clan
    root. Spouses sit next to the primary person.

    Returns:
      {
        "nodes": [ { person_id, x, y, w, h, kind: 'person'|'spouse', spouse_of? } ],
        "edges": [ { kind: 'parent'|'spouse', from: pid, to: pid } ],
        "width": float, "height": float,
      }
    """
    families_by_id = {f["family_id"]: f for f in family_payload}

    # children of a person = union of children across all their spouse-families
    def children_of(pid: str) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        seen: set[str] = set()
        for fid in spouse_families_by_person.get(pid, []):
            fam = families_by_id.get(fid)
            if not fam:
                continue
            for cid in fam["children"]:
                if cid in seen:
                    continue
                seen.add(cid)
                out.append((cid, fid))
        return out

    def spouses_of(pid: str) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        for fid in spouse_families_by_person.get(pid, []):
            fam = families_by_id.get(fid)
            if not fam:
                continue
            for sid in fam["spouses"]:
                if sid != pid:
                    out.append((sid, fid))
        return out

    nodes: list[dict] = []
    edges: list[dict] = []
    placed: set[str] = set()

    # Measure subtree width recursively (width in pixels; height derived from depth later).
    # Approach: person occupies CARD_W + (spouses count) * (CARD_W + SPOUSE_GAP).
    # Children stack horizontally below; subtree width = max(own width, sum of child widths + gaps).

    def own_width(pid: str) -> float:
        spouse_count = len(spouses_of(pid))
        return OVERVIEW_CARD_W + spouse_count * (OVERVIEW_CARD_W + OVERVIEW_SPOUSE_GAP)

    measure_cache: dict[str, float] = {}

    def measure(pid: str, stack: tuple[str, ...]) -> float:
        if pid in measure_cache:
            return measure_cache[pid]
        if pid in stack:
            return own_width(pid)
        kids = children_of(pid)
        if not kids:
            w = own_width(pid)
        else:
            # Layout right-packs leaf siblings next to big ones. Measure the
            # total width as the sum of all children widths, because every
            # child still occupies its own horizontal slot even when leaves
            # are glued to the right of the big group rather than being
            # centered separately.
            child_w = 0.0
            for cid, _ in kids:
                child_w += measure(cid, stack + (pid,))
            child_w += OVERVIEW_SIB_GAP * max(0, len(kids) - 1)
            w = max(own_width(pid), child_w)
        measure_cache[pid] = w
        return w

    def layout(pid: str, left: float, top: float, stack: tuple[str, ...]) -> None:
        if pid in placed or pid in stack:
            return
        placed.add(pid)
        w = measure(pid, stack)
        ow = own_width(pid)
        # center the pair-row within our subtree width
        row_left = left + (w - ow) / 2
        nodes.append({
            "person_id": pid,
            "kind": "person",
            "x": round(row_left, 2),
            "y": round(top, 2),
            "w": OVERVIEW_CARD_W,
            "h": OVERVIEW_CARD_H,
        })
        cur_x = row_left + OVERVIEW_CARD_W
        sps = spouses_of(pid)
        for sid, fid in sps:
            cur_x += OVERVIEW_SPOUSE_GAP
            nodes.append({
                "person_id": sid,
                "kind": "spouse",
                "x": round(cur_x, 2),
                "y": round(top, 2),
                "w": OVERVIEW_CARD_W,
                "h": OVERVIEW_CARD_H,
                "spouse_of": pid,
                "family_id": fid,
            })
            edges.append({"kind": "spouse", "from": pid, "to": sid})
            cur_x += OVERVIEW_CARD_W
        # children row — standard centered layout; leaf-only nodes are pulled
        # closer to their big siblings in a post-pass (`_repack_leaves`).
        kids = children_of(pid)
        if kids:
            child_row_y = top + OVERVIEW_CARD_H + OVERVIEW_GEN_GAP
            widths = [measure(cid, stack + (pid,)) for cid, _ in kids]
            total = sum(widths) + OVERVIEW_SIB_GAP * max(0, len(kids) - 1)
            cursor = left + (w - total) / 2
            for (cid, _), cw_ in zip(kids, widths, strict=True):
                edges.append({"kind": "parent", "from": pid, "to": cid})
                layout(cid, cursor, child_row_y, stack + (pid,))
                cursor += cw_ + OVERVIEW_SIB_GAP

    # Order roots by clan size (largest first) already done in root_persons.
    # Skip tiny orphan clans (single persons) in overview — they're available via sidebar/search.
    clan_members: dict[str, list[str]] = defaultdict(list)
    for pid, rid in person_roots.items():
        clan_members[rid].append(pid)
    cursor_x = OVERVIEW_PADDING
    max_bottom = OVERVIEW_PADDING
    for rid in root_persons:
        if rid in placed:
            continue
        if len(clan_members.get(rid, [])) < 2:
            continue
        w = measure(rid, ())
        layout(rid, cursor_x, OVERVIEW_PADDING, ())
        cursor_x += w + OVERVIEW_BRANCH_GAP

    # Post-pass: re-pack leaf descendants (persons with no own children and no
    # spouse) next to their closest big sibling, dramatically reducing empty
    # horizontal gaps without disturbing the skeleton of big subtrees.
    _repack_leaves(
        nodes, parent_family_by_person, families_by_id, children_of, spouses_of
    )
    # compute canvas size from placed nodes
    min_x = min((n["x"] for n in nodes), default=0)
    max_x = max((n["x"] + n["w"] for n in nodes), default=OVERVIEW_PADDING)
    min_y = min((n["y"] for n in nodes), default=0)
    max_y = max((n["y"] + n["h"] for n in nodes), default=OVERVIEW_PADDING)
    return {
        "nodes": nodes,
        "edges": edges,
        "width": round(max_x - min_x + OVERVIEW_PADDING * 2, 2),
        "height": round(max_y - min_y + OVERVIEW_PADDING * 2, 2),
        "offsetX": round(-min_x + OVERVIEW_PADDING, 2),
        "offsetY": round(-min_y + OVERVIEW_PADDING, 2),
    }


def _repack_leaves(nodes, parent_family_by_person, families_by_id, children_of_fn, spouses_of_fn):
    """Move leaf persons (no children, no spouse) horizontally toward their
    closest big sibling so that sparse branches don't drift into the far edge
    of a massive brother's subtree. The move is bounded so no overlaps occur.
    """
    person_nodes = {n["person_id"]: n for n in nodes if n.get("kind") == "person"}
    # Build row index: list of nodes at each y, sorted by x.
    by_y: dict[float, list[dict]] = defaultdict(list)
    for n in nodes:
        by_y[n["y"]].append(n)
    for row in by_y.values():
        row.sort(key=lambda n: n["x"])

    for pid, node in list(person_nodes.items()):
        pfid = parent_family_by_person.get(pid)
        if not pfid:
            continue
        fam = families_by_id.get(pfid)
        if not fam:
            continue
        siblings = [cid for cid in fam["children"] if cid != pid and cid in person_nodes]
        if not siblings:
            continue
        # is this person a leaf? no children and no spouses
        has_own_kids = bool(children_of_fn(pid))
        has_spouses = bool(spouses_of_fn(pid))
        if has_own_kids or has_spouses:
            continue
        # find the closest big sibling (one with own kids)
        big_sibs = [sid for sid in siblings if children_of_fn(sid)]
        if not big_sibs:
            continue
        my_x = node["x"]
        closest = min(big_sibs, key=lambda sid: abs(person_nodes[sid]["x"] - my_x))
        target_x = person_nodes[closest]["x"] + person_nodes[closest]["w"] + OVERVIEW_SIB_GAP
        if target_x >= my_x - 1:
            # closest big is already to the left and near — no move needed
            if abs(my_x - target_x) < 4:
                continue
        # find row constraint: closest left neighbor (other than self) on same y
        row = by_y[node["y"]]
        idx = row.index(node)
        left_bound = OVERVIEW_PADDING
        if idx > 0:
            left = row[idx - 1]
            left_bound = left["x"] + left["w"] + OVERVIEW_SIB_GAP
        right_bound = 10**9
        if idx < len(row) - 1:
            right = row[idx + 1]
            right_bound = right["x"] - OVERVIEW_SIB_GAP
        # Clamp target into [left_bound, right_bound - w]
        final_x = max(left_bound, min(target_x, right_bound - node["w"]))
        if abs(final_x - my_x) < 4:
            continue
        node["x"] = round(final_x, 2)
        # keep row sorted for subsequent leaves on the same y
        row.sort(key=lambda n: n["x"])


def _short_name(name: str) -> str:
    # Keep first 2 tokens for compact card headline
    parts = name.split()
    if not parts:
        return name
    if len(parts) == 1:
        return parts[0]
    return " ".join(parts[:2])


def _initials(name: str) -> str:
    parts = [p for p in name.split() if p]
    if not parts:
        return "?"
    if len(parts) == 1:
        return parts[0][:1].upper()
    return (parts[0][:1] + parts[1][:1]).upper()


def _compute_root_persons(
    person_by_id: dict[str, dict],
    parent_family_by_person: dict[str, str],
    families: list[dict],
    spouse_families_by_person: dict[str, list[str]],
    children_by_family: dict[str, list[str]],
) -> tuple[list[str], dict[str, str]]:
    """Return canonical root persons and a person -> root_id mapping.

    Persons sharing a clan (connected through spouse+ancestor edges) are grouped
    into a single canonical root (the eldest known ancestor of that clan).
    """
    # Union-find over all persons.
    parent_of: dict[str, str] = {pid: pid for pid in person_by_id}

    def find(x: str) -> str:
        root = x
        while parent_of[root] != root:
            root = parent_of[root]
        while parent_of[x] != root:
            parent_of[x], x = root, parent_of[x]
        return root

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent_of[rb] = ra

    # Parent-child edges connect clans.
    for cid, fid in parent_family_by_person.items():
        # family's parents (spouses) all share clan with the child
        for fam in families:
            if fam["family_id"] == fid:
                for sid in (fam["spouse1_person_id"], fam["spouse2_person_id"]):
                    if sid and sid in person_by_id:
                        union(sid, cid)
                break
    # Spouse pairing also shares a clan (marriage joins them).
    for fam in families:
        a, b = fam["spouse1_person_id"], fam["spouse2_person_id"]
        if a and b and a in person_by_id and b in person_by_id:
            union(a, b)

    clans: dict[str, list[str]] = defaultdict(list)
    for pid in person_by_id:
        clans[find(pid)].append(pid)

    # Precompute descendant count per person (via spouse families + children).
    def descendant_count(start: str) -> int:
        seen = {start}
        stack = [start]
        count = 0
        while stack:
            cur = stack.pop()
            for fid in spouse_families_by_person.get(cur, []):
                for cid in children_by_family.get(fid, []):
                    if cid not in seen:
                        seen.add(cid)
                        count += 1
                        stack.append(cid)
        return count

    # For each clan pick the "canonical" ancestor.
    # Preference: (1) no parent family, (2) largest descendant subtree,
    # (3) earliest birth year, (4) smallest person_id.
    person_roots: dict[str, str] = {}
    root_list: list[str] = []
    for members in clans.values():
        rootless = [m for m in members if m not in parent_family_by_person]
        pool = rootless if rootless else members
        canon = max(
            pool,
            key=lambda pid: (
                descendant_count(pid),
                -int(_parse_year(person_by_id[pid].get("birth")) or "9999"),
                -int(pid[1:]) if pid[1:].isdigit() else 0,
            ),
        )
        root_list.append(canon)
        for m in members:
            person_roots[m] = canon

    root_list.sort(key=lambda pid: (-_clan_size(pid, clans, person_roots), pid))
    return root_list, person_roots


def _clan_size(pid: str, clans: dict[str, list[str]], person_roots: dict[str, str]) -> int:
    for members in clans.values():
        if pid in members:
            return len(members)
    return 0


def _select_default_focus(
    person_by_id: dict[str, dict],
    children_by_family: dict[str, list[str]],
    spouse_families_by_person: dict[str, list[str]],
    parent_family_by_person: dict[str, str],
    root_persons: list[str],
) -> str:
    if not person_by_id:
        return ""

    def descendant_count(pid: str) -> int:
        seen = set()
        stack = [pid]
        total = 0
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            for fid in spouse_families_by_person.get(current, []):
                for child_id in children_by_family.get(fid, []):
                    if child_id not in seen:
                        total += 1
                        stack.append(child_id)
        return total

    candidates = root_persons or list(person_by_id.keys())
    best = max(candidates, key=lambda pid: (descendant_count(pid), -int(_parse_year(person_by_id[pid].get("birth")) or 9999)))
    return best


def _parse_year(value: str | None) -> str:
    if not value:
        return ""
    match = re.search(r"(\d{4})", value)
    return match.group(1) if match else ""


def _short_year_label(person: dict) -> str:
    start = _parse_year(person.get("birth")) or _parse_year(person.get("christening"))
    end = _parse_year(person.get("death"))
    if start and end:
        return f"{start}-{end}"
    if start:
        return start
    if end:
        return f"?-{end}"
    return ""


def _wrap_label(text: str, max_chars: int, max_lines: int) -> list[str]:
    words = text.split()
    if not words:
        return [text]
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        candidate = f"{current} {word}"
        if len(candidate) <= max_chars:
            current = candidate
        else:
            lines.append(current)
            current = word
            if len(lines) == max_lines - 1:
                break
    lines.append(current)
    remainder = words[len(" ".join(lines).split()):]
    if remainder:
        lines[-1] = (lines[-1] + " " + " ".join(remainder)).strip()
    if len(lines[-1]) > max_chars + 6:
        lines[-1] = lines[-1][: max_chars + 3].rstrip() + "..."
    return lines[:max_lines]


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Genealogy Tree</title>
<style>
:root {
  color-scheme: light;
  --bg: #f7f6f3;
  --surface: #ffffff;
  --border: #e5e2db;
  --border-strong: #c9c4b8;
  --ink: #2b2a26;
  --ink-muted: #6b6357;
  --ink-faint: #8a8376;
  --edge: #b8b3a8;
  --edge-strong: #7a7266;
  --accent: #b4824a;
  --accent-soft: #e7d3ba;
  --focus-ring: #d4a574;
  --male: #4a6fa5;
  --female: #a85a7a;
  --unknown: #9a9288;
  --chip-bg: #efe9df;
  --card-w: 180;
  --card-h: 72;
  --focus-w: 220;
  --focus-h: 88;
  --sib-gap: 28;
  --spouse-gap: 28;
  --gen-gap: 100;
  font: 14px/1.4 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  font-variant-numeric: tabular-nums;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--ink); }
header { padding: 10px 18px; background: var(--surface); border-bottom: 1px solid var(--border); display: flex; gap: 12px; align-items: center; }
h1 { font-size: 17px; font-weight: 600; margin: 0; white-space: nowrap; letter-spacing: 0.2px; }
#drawer-toggle { display: none; }
.toolbar { display: inline-flex; gap: 6px; margin-left: auto; align-items: center; }
.icon-btn { width: 34px; height: 34px; border: 1px solid var(--border-strong); border-radius: 6px; background: var(--surface); cursor: pointer; font: inherit; font-size: 16px; color: var(--ink); display: inline-flex; align-items: center; justify-content: center; }
.icon-btn:hover { background: var(--bg); }
.icon-btn[disabled] { opacity: 0.35; cursor: default; }
main { display: grid; grid-template-columns: 280px minmax(0, 1fr) 320px; height: calc(100vh - 55px); transition: grid-template-columns 240ms cubic-bezier(0.4, 0, 0.2, 1); }
#sidebar { background: var(--surface); border-right: 1px solid var(--border); overflow: hidden; display: flex; flex-direction: column; position: relative; }
#search { width: calc(100% - 24px); margin: 12px; padding: 9px 11px; border: 1px solid var(--border-strong); border-radius: 6px; font: inherit; background: var(--bg); }
#search:focus { outline: 2px solid var(--focus-ring); outline-offset: -1px; }
#people-list { flex: 1; overflow-y: auto; padding: 0 6px 12px; }
.root-group { margin-bottom: 14px; }
.root-group h3 { margin: 6px 10px 4px; font-size: 11px; font-weight: 600; letter-spacing: 0.6px; text-transform: uppercase; color: var(--ink-faint); }
.person-item { display: flex; justify-content: space-between; align-items: baseline; gap: 8px; padding: 5px 10px; border-radius: 4px; cursor: pointer; color: var(--ink); }
.person-item:hover { background: var(--bg); }
.person-item.current { background: var(--accent-soft); color: var(--ink); }
.person-item .years { color: var(--ink-muted); font-size: 12px; font-variant-numeric: tabular-nums; }
#tree-area { display: flex; flex-direction: column; min-width: 0; overflow: hidden; background: var(--bg); }
#breadcrumb { padding: 10px 16px; font-size: 13px; color: var(--ink-muted); border-bottom: 1px solid var(--border); background: var(--surface); overflow-x: auto; white-space: nowrap; min-height: 40px; }
#breadcrumb .crumb { color: var(--ink-muted); cursor: pointer; text-decoration: none; }
#breadcrumb .crumb:hover { color: var(--accent); text-decoration: underline; }
#breadcrumb .crumb.current { color: var(--ink); font-weight: 600; cursor: default; }
#breadcrumb .sep { margin: 0 6px; color: var(--ink-faint); }
#tree-shell { position: relative; flex: 1; overflow: hidden; }
#tree-svg { width: 100%; height: 100%; display: block; cursor: grab; user-select: none; -webkit-user-select: none; background: var(--bg); }
#tree-svg.dragging { cursor: grabbing; }
#viewport { transition: transform 320ms cubic-bezier(0.4, 0, 0.2, 1); }
#viewport.no-anim { transition: none; }
#detail { background: var(--surface); border-left: 1px solid var(--border); padding: 18px 20px; overflow-y: auto; position: relative; min-width: 0; }
.empty-state { color: var(--ink-muted); font-size: 13px; padding: 8px 0; }
.detail-header { margin-bottom: 14px; }
.detail-header h2 { margin: 0 0 4px; font-size: 20px; font-weight: 600; letter-spacing: 0.1px; }
.muted { color: var(--ink-muted); font-size: 12px; }
.detail-row { margin: 6px 0; font-size: 13px; }
.detail-row strong { color: var(--ink-muted); font-weight: 500; margin-right: 6px; }
.photo { max-width: 100%; border-radius: 4px; border: 1px solid var(--border); margin: 10px 0 6px; display: block; }
figcaption { font-size: 12px; color: var(--ink-muted); }

.card-bg { fill: var(--surface); stroke: var(--border-strong); stroke-width: 1; }
.card-bg.focus { stroke: var(--focus-ring); stroke-width: 2; }
.card-bg.ancestor { fill: #fbfaf7; }
.card-bg.sibling { fill: #f4f1ea; }
.card-accent { stroke-width: 3; }
.card-accent.male { stroke: var(--male); }
.card-accent.female { stroke: var(--female); }
.card-accent.unknown { stroke: var(--unknown); }
.label-name { fill: var(--ink); font-size: 13px; font-weight: 600; }
.label-name.focus { font-size: 14px; }
.label-name.sibling { font-size: 12px; fill: var(--ink-muted); }
.label-year { fill: var(--ink-muted); font-size: 11px; font-variant-numeric: tabular-nums; }
.edge { fill: none; stroke: var(--edge); stroke-width: 1.4; }
.edge.highlight { stroke: var(--edge-strong); stroke-width: 1.8; }
.spouse-pair { stroke: var(--edge-strong); stroke-width: 2; stroke-linecap: round; }
.chip { cursor: pointer; }
.chip .chip-bg { fill: var(--chip-bg); stroke: var(--border-strong); stroke-width: 1; }
.chip:hover .chip-bg { fill: var(--accent-soft); }
.chip .chip-label { fill: var(--ink-muted); font-size: 11px; font-weight: 500; }
.card-hit { fill: transparent; cursor: pointer; }
.card-group:hover .card-bg { stroke: var(--edge-strong); }
.avatar-bg { fill: var(--accent-soft); }
.avatar-initials { fill: var(--accent); font-size: 11px; font-weight: 600; }
.mode-tabs { display: inline-flex; border: 1px solid var(--border-strong); border-radius: 6px; overflow: hidden; background: var(--surface); }
.mode-tabs button { border: 0; padding: 7px 12px; background: transparent; cursor: pointer; color: var(--ink-muted); font: inherit; font-size: 13px; border-right: 1px solid var(--border); }
.mode-tabs button:last-child { border-right: 0; }
.mode-tabs button.active { background: var(--accent-soft); color: var(--ink); font-weight: 600; }
.mode-tabs button:hover:not(.active) { background: var(--bg); color: var(--ink); }
.pagination-controls { display: inline-flex; gap: 4px; align-items: center; margin-left: 8px; }
.pagination-controls label { font-size: 12px; color: var(--ink-muted); }
.pagination-controls select { padding: 6px 8px; border: 1px solid var(--border-strong); border-radius: 6px; background: var(--surface); font: inherit; font-size: 13px; }
.ov-card-bg { fill: var(--surface); stroke: var(--border-strong); stroke-width: 1; }
.ov-card-bg.active { fill: var(--accent-soft); stroke: var(--focus-ring); stroke-width: 2; }
.ov-card-bg.spouse { fill: #fbfaf7; }
.ov-name { fill: var(--ink); font-size: 11px; font-weight: 600; }
.ov-year { fill: var(--ink-muted); font-size: 10px; font-variant-numeric: tabular-nums; }

@media (min-width: 961px) {
  .sidebar-collapse-btn { position: absolute; top: 10px; width: 26px; height: 28px; padding: 0; font-size: 14px; line-height: 1; z-index: 3; }
  #sidebar .sidebar-collapse-btn { right: 6px; }
  #detail .sidebar-collapse-btn { left: 6px; }
  main.left-collapsed { grid-template-columns: 40px minmax(0, 1fr) 320px; }
  main.right-collapsed { grid-template-columns: 280px minmax(0, 1fr) 40px; }
  main.left-collapsed.right-collapsed { grid-template-columns: 40px minmax(0, 1fr) 40px; }
  main.left-collapsed #sidebar > :not(.sidebar-collapse-btn) { display: none; }
  main.right-collapsed #detail > :not(.sidebar-collapse-btn) { display: none; }
}
@media (max-width: 960px) {
  main { grid-template-columns: 280px 1fr; }
  .sidebar-collapse-btn { display: none; }
  #detail { display: none; }
  #detail.open { display: block; position: fixed; right: 0; top: 55px; bottom: 0; width: min(340px, 94vw); z-index: 12; box-shadow: -4px 0 16px rgba(0,0,0,0.06); }
}
@media (max-width: 720px) {
  main { grid-template-columns: 1fr; }
  #drawer-toggle { display: inline-flex; }
  #sidebar { position: fixed; left: 0; top: 55px; bottom: 0; width: min(300px, 88vw); z-index: 10; transform: translateX(-100%); transition: transform 240ms; box-shadow: 4px 0 16px rgba(0,0,0,0.08); }
  #sidebar.open { transform: translateX(0); }
  #detail.open { width: min(340px, 94vw); }
}
</style>
</head>
<body>
<header>
  <button id="drawer-toggle" class="icon-btn" type="button" title="Browse people" aria-label="Browse people">&#9776;</button>
  <h1>Genealogy Tree</h1>
  <div class="mode-tabs" role="tablist">
    <button id="mode-overview" type="button" data-mode="overview" title="Whole tree">Overview</button>
    <button id="mode-pagination" type="button" data-mode="pagination" title="Generation-by-generation">Generations</button>
    <button id="mode-focus" type="button" data-mode="focus" title="Focus on one person">Focus</button>
  </div>
  <div id="pagination-controls" class="pagination-controls" hidden>
    <label for="gen-depth">Generations:</label>
    <select id="gen-depth">
      <option value="2">2</option>
      <option value="3" selected>3</option>
      <option value="4">4</option>
      <option value="5">5</option>
    </select>
    <button id="gen-up" class="icon-btn" type="button" title="Up to parents" aria-label="Up">&#8593;</button>
    <button id="gen-down" class="icon-btn" type="button" title="Down to first child" aria-label="Down">&#8595;</button>
  </div>
  <div class="toolbar">
    <button id="nav-back" class="icon-btn" type="button" title="Back" aria-label="Back">&#8592;</button>
    <button id="nav-forward" class="icon-btn" type="button" title="Forward" aria-label="Forward">&#8594;</button>
    <button id="zoom-in" class="icon-btn" type="button" title="Zoom in" aria-label="Zoom in">+</button>
    <button id="zoom-out" class="icon-btn" type="button" title="Zoom out" aria-label="Zoom out">&minus;</button>
    <button id="reset-view" class="icon-btn" type="button" title="Fit to view" aria-label="Fit to view">&#8962;</button>
    <button id="detail-toggle" class="icon-btn" type="button" title="Person details" aria-label="Person details">i</button>
  </div>
</header>
<main>
  <aside id="sidebar" aria-label="People list">
    <button id="sidebar-collapse" class="icon-btn sidebar-collapse-btn" type="button" title="Collapse sidebar" aria-label="Collapse sidebar" aria-expanded="true">&#8249;</button>
    <input id="search" type="search" placeholder="Search people" autocomplete="off">
    <div id="people-list"></div>
  </aside>
  <section id="tree-area">
    <nav id="breadcrumb" aria-label="Ancestry path"></nav>
    <div id="tree-shell">
      <svg id="tree-svg" aria-label="Family tree">
        <defs>
          <clipPath id="avatar-clip" clipPathUnits="objectBoundingBox"><circle cx="0.5" cy="0.5" r="0.5"/></clipPath>
        </defs>
        <g id="viewport"></g>
      </svg>
    </div>
  </section>
  <aside id="detail" aria-label="Person details">
    <button id="detail-collapse" class="icon-btn sidebar-collapse-btn" type="button" title="Collapse details" aria-label="Collapse details" aria-expanded="true">&#8250;</button>
    <div class="empty-state">Select a person to view details.</div>
  </aside>
</main>
<script id="tree-data" type="application/json">__DATA__</script>
<script>
(() => {
const data = JSON.parse(document.getElementById('tree-data').textContent);
const SVG_NS = 'http://www.w3.org/2000/svg';
const people = new Map(data.persons.map(p => [p.person_id, p]));
const families = new Map(data.families.map(f => [f.family_id, f]));
const parentFamily = data.parentFamily || {};
const spouseFamilies = data.spouseFamilies || {};
const rootPersons = data.rootPersons || [];
const personRoots = data.personRoots || {};

const svg = document.getElementById('tree-svg');
const viewport = document.getElementById('viewport');
const detail = document.getElementById('detail');
const breadcrumb = document.getElementById('breadcrumb');
const peopleList = document.getElementById('people-list');
const searchInput = document.getElementById('search');
const sidebar = document.getElementById('sidebar');

const CARD_W = 180, CARD_H = 72;
const FOCUS_W = 220, FOCUS_H = 88;
const SIB_W = 150, SIB_H = 56;
const SIB_GAP = 22;
const SPOUSE_GAP = 24;
const SIBLING_GROUP_GAP = 44;
const GEN_GAP = 110;
const CHIP_W = 120, CHIP_H = 28;
const EXPAND_STORAGE = 'genealogy.expanded.v1';
const FOCUS_STORAGE = 'genealogy.focus.v1';
const SIDEBAR_COLLAPSED_KEY = 'genealogy.sidebar.collapsed.v1';
const DETAIL_COLLAPSED_KEY = 'genealogy.detail.collapsed.v1';
const mainEl = document.querySelector('main');
const sidebarCollapseBtn = document.getElementById('sidebar-collapse');
const detailCollapseBtn = document.getElementById('detail-collapse');
let bootComplete = false;
let detailEphemerallyExpanded = false;

let focusId = null;
let expandedChildren = new Set();
let historyStack = [];
let historyIndex = -1;
let currentScene = null;
let viewScale = 1;
let viewTranslateX = 0;
let viewTranslateY = 0;
let dragState = null;

try {
  const savedExpanded = JSON.parse(localStorage.getItem(EXPAND_STORAGE) || '[]');
  if (Array.isArray(savedExpanded)) expandedChildren = new Set(savedExpanded);
} catch {}

function svgEl(name, attrs) {
  const node = document.createElementNS(SVG_NS, name);
  if (attrs) for (const k in attrs) node.setAttribute(k, attrs[k]);
  return node;
}
function htmlEl(name, attrs) {
  const node = document.createElement(name);
  if (attrs) for (const k in attrs) node.setAttribute(k, attrs[k]);
  return node;
}
function isSafeImageSrc(value) {
  if (!value) return false;
  try {
    const url = new URL(value, window.location.href);
    if (url.protocol === 'http:' || url.protocol === 'https:') return true;
    return !/^[a-z][a-z0-9+.-]*:/i.test(value);
  } catch { return false; }
}
function personYears(p) { return p && p.years || ''; }

// ---- Scene computation ----
// A "scene" is the subgraph rendered for the current focus:
// - focus person (central)
// - focus's spouses (same row)
// - focus's siblings (same row, compact)
// - focus's parents, grandparents (up to 3 ancestor generations)
// - focus's children (one row below), with chips for deeper descendants unless expanded
//
// Nodes: { id, kind, personId?, x, y, w, h, row }
// Edges: { kind: 'parent-child'|'spouse', from: nodeId, to: nodeId }

function siblingsOf(personId) {
  const parentId = parentFamily[personId];
  if (!parentId) return [];
  const fam = families.get(parentId);
  if (!fam) return [];
  return fam.children.filter(id => id !== personId);
}

function spousesOf(personId) {
  const pairs = [];
  const fams = spouseFamilies[personId] || [];
  for (const fid of fams) {
    const fam = families.get(fid);
    if (!fam) continue;
    for (const sid of fam.spouses) if (sid !== personId) pairs.push({ spouseId: sid, familyId: fid });
  }
  return pairs;
}

function childrenOf(personId) {
  // All children across all spouse-families (deduped, preserving order)
  const seen = new Set();
  const list = [];
  for (const fid of spouseFamilies[personId] || []) {
    const fam = families.get(fid);
    if (!fam) continue;
    for (const cid of fam.children) {
      if (!seen.has(cid)) { seen.add(cid); list.push({ childId: cid, familyId: fid }); }
    }
  }
  return list;
}

function parentsOf(personId) {
  const parentId = parentFamily[personId];
  if (!parentId) return null;
  const fam = families.get(parentId);
  if (!fam) return null;
  return { familyId: parentId, spouses: fam.spouses.slice() };
}

function computeScene(pid) {
  const scene = { nodes: new Map(), edges: [], focusId: pid, order: [] };
  const put = (node) => { scene.nodes.set(node.id, node); scene.order.push(node.id); return node; };

  // Row 0 = focus. Rows negative = ancestors, positive = descendants.
  put({ id: 'p:' + pid, kind: 'focus', personId: pid, row: 0 });

  // Spouses (row 0)
  const focusSpouses = spousesOf(pid);
  focusSpouses.forEach((s, i) => {
    put({ id: 'p:' + s.spouseId + ':spouse:' + i, kind: 'spouse', personId: s.spouseId, row: 0, familyId: s.familyId });
    scene.edges.push({ kind: 'spouse', from: 'p:' + pid, to: 'p:' + s.spouseId + ':spouse:' + i });
  });

  // Siblings (row 0, compact)
  const sibs = siblingsOf(pid);
  sibs.forEach((sid, i) => {
    put({ id: 'p:' + sid + ':sibling', kind: 'sibling', personId: sid, row: 0 });
  });

  // Parents (row -1)
  const parents = parentsOf(pid);
  if (parents) {
    parents.spouses.forEach((spid, i) => {
      put({ id: 'p:' + spid + ':parent:' + i, kind: 'ancestor', personId: spid, row: -1 });
    });
    // Grandparents (row -2) — up to one line of ancestors per known parent
    parents.spouses.forEach((spid, i) => {
      const gp = parentsOf(spid);
      if (!gp) return;
      gp.spouses.forEach((gspid, j) => {
        put({ id: 'p:' + gspid + ':gparent:' + i + ':' + j, kind: 'ancestor', personId: gspid, row: -2 });
      });
    });
  }

  // Children (row 1)
  const kids = childrenOf(pid);
  kids.forEach(k => {
    put({ id: 'p:' + k.childId + ':child', kind: 'child', personId: k.childId, row: 1, familyId: k.familyId });
    // Grandchildren chip OR expanded list (row 2)
    const expanded = expandedChildren.has(k.childId);
    const grandKids = childrenOf(k.childId);
    if (grandKids.length) {
      if (expanded) {
        grandKids.forEach((gk, gi) => {
          put({ id: 'p:' + gk.childId + ':gchild:' + gi, kind: 'grandchild', personId: gk.childId, row: 2, parentChildId: k.childId });
        });
      } else {
        put({ id: 'chip:' + k.childId, kind: 'chip', parentId: k.childId, count: grandKids.length, row: 2 });
      }
    }
  });

  return scene;
}

// ---- Layout ----
// Left-to-right within each row, vertically stacked by row.
// Focus row: siblings | [parents of focus connector] | focus + spouses.
// Siblings are smaller (SIB_W x SIB_H). Focus is FOCUS_W x FOCUS_H. Spouses are CARD_W x CARD_H.

function layoutScene(scene) {
  // Group nodes by row
  const rows = new Map();
  for (const id of scene.order) {
    const n = scene.nodes.get(id);
    if (!rows.has(n.row)) rows.set(n.row, []);
    rows.get(n.row).push(n);
  }
  const rowKeys = [...rows.keys()].sort((a, b) => a - b);

  // Assign sizes
  for (const n of scene.nodes.values()) {
    if (n.kind === 'focus') { n.w = FOCUS_W; n.h = FOCUS_H; }
    else if (n.kind === 'sibling') { n.w = SIB_W; n.h = SIB_H; }
    else if (n.kind === 'chip') { n.w = CHIP_W; n.h = CHIP_H; }
    else { n.w = CARD_W; n.h = CARD_H; }
  }

  // --- Row 0: siblings (left) | focus + spouses (center) ---
  const row0 = rows.get(0) || [];
  const siblings = row0.filter(n => n.kind === 'sibling');
  const focusNode = row0.find(n => n.kind === 'focus');
  const spouses = row0.filter(n => n.kind === 'spouse');

  let x = 0;
  const siblingsLeft = x;
  siblings.forEach((n, i) => {
    if (i > 0) x += SIB_GAP;
    n.x = x; n.y = -n.h / 2;
    x += n.w;
  });
  const siblingsRight = x;
  if (siblings.length) x += SIBLING_GROUP_GAP;

  const focusLeft = x;
  focusNode.x = x; focusNode.y = -focusNode.h / 2;
  x += focusNode.w;
  spouses.forEach((n) => {
    x += SPOUSE_GAP;
    n.x = x; n.y = -n.h / 2;
    x += n.w;
  });
  const focusRight = x;
  const focusCenterX = (focusLeft + focusRight) / 2;

  // --- Row -1: parents, centered over focus (not including spouses) ---
  const rowM1 = rows.get(-1) || [];
  if (rowM1.length) {
    const totalW = rowM1.reduce((s, n) => s + n.w, 0) + SPOUSE_GAP * Math.max(0, rowM1.length - 1);
    let px = focusLeft + (focusNode.w - totalW) / 2;
    rowM1.forEach((n, i) => {
      if (i > 0) px += SPOUSE_GAP;
      n.x = px; n.y = -GEN_GAP - n.h / 2;
      px += n.w;
    });
  }

  // --- Row -2: grandparents, split beneath each parent ---
  const rowM2 = rows.get(-2) || [];
  if (rowM2.length && rowM1.length) {
    // Group by parent index (from id "...:gparent:{parentIdx}:{j}")
    const byParent = new Map();
    for (const n of rowM2) {
      const m = n.id.match(/:gparent:(\\d+):(\\d+)$/);
      const idx = m ? parseInt(m[1], 10) : 0;
      if (!byParent.has(idx)) byParent.set(idx, []);
      byParent.get(idx).push(n);
    }
    for (const [idx, group] of byParent) {
      const parent = rowM1[idx];
      if (!parent) continue;
      const totalW = group.reduce((s, n) => s + n.w, 0) + SPOUSE_GAP * Math.max(0, group.length - 1);
      let gx = parent.x + (parent.w - totalW) / 2;
      group.forEach((n, i) => {
        if (i > 0) gx += SPOUSE_GAP;
        n.x = gx; n.y = -2 * GEN_GAP - n.h / 2;
        gx += n.w;
      });
    }
  }

  // --- Row 1: children, centered under focus+spouses ---
  const row1 = rows.get(1) || [];
  if (row1.length) {
    const totalW = row1.reduce((s, n) => s + n.w, 0) + SIB_GAP * (row1.length - 1);
    let cx = focusCenterX - totalW / 2;
    row1.forEach((n, i) => {
      if (i > 0) cx += SIB_GAP;
      n.x = cx; n.y = GEN_GAP - n.h / 2;
      cx += n.w;
    });
  }

  // --- Row 2: chips or grandchildren under each child ---
  const row2 = rows.get(2) || [];
  // Group by parentChildId or chip.parentId
  const gcByParent = new Map();
  for (const n of row2) {
    const pid = n.kind === 'chip' ? n.parentId : n.parentChildId;
    if (!gcByParent.has(pid)) gcByParent.set(pid, []);
    gcByParent.get(pid).push(n);
  }
  for (const [pid, group] of gcByParent) {
    const childNode = row1.find(n => n.personId === pid);
    if (!childNode) continue;
    const totalW = group.reduce((s, n) => s + n.w, 0) + SIB_GAP * Math.max(0, group.length - 1);
    let gx = childNode.x + childNode.w / 2 - totalW / 2;
    group.forEach((n, i) => {
      if (i > 0) gx += SIB_GAP;
      n.x = gx; n.y = 2 * GEN_GAP - n.h / 2;
      gx += n.w;
    });
  }

  // --- Edges ---
  scene.edges = [];
  // spouse pairs
  for (const s of spouses) scene.edges.push({ kind: 'spouse', from: focusNode, to: s });
  // parent -> focus
  for (const p of rowM1) scene.edges.push({ kind: 'parent-child', from: p, to: focusNode });
  // parent-sibling: parents connect to siblings too (visually shows they're full siblings)
  if (rowM1.length) {
    for (const sib of siblings) scene.edges.push({ kind: 'parent-child', from: rowM1[0], to: sib });
  }
  // grandparent -> parent
  for (const gp of rowM2) {
    const m = gp.id.match(/:gparent:(\\d+):/);
    const pi = m ? parseInt(m[1], 10) : 0;
    const parent = rowM1[pi];
    if (parent) scene.edges.push({ kind: 'parent-child', from: gp, to: parent });
  }
  // focus -> child
  for (const c of row1) scene.edges.push({ kind: 'parent-child', from: focusNode, to: c });
  // child -> grandchild/chip
  for (const n of row2) {
    const parentId = n.kind === 'chip' ? n.parentId : n.parentChildId;
    const childNode = row1.find(rn => rn.personId === parentId);
    if (childNode) scene.edges.push({ kind: 'parent-child', from: childNode, to: n });
  }

  // Compute bbox for viewport fitting
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const n of scene.nodes.values()) {
    minX = Math.min(minX, n.x);
    minY = Math.min(minY, n.y);
    maxX = Math.max(maxX, n.x + n.w);
    maxY = Math.max(maxY, n.y + n.h);
  }
  scene.bbox = { minX, minY, maxX, maxY, width: maxX - minX, height: maxY - minY };
  return scene;
}

// ---- Rendering ----

function orthogonalPath(fromX, fromY, toX, toY, kind) {
  // Parent-child: vertical down, horizontal bar at midpoint, vertical to child.
  // Spouse: short horizontal line between cards.
  if (kind === 'spouse') {
    return 'M ' + fromX + ' ' + fromY + ' L ' + toX + ' ' + toY;
  }
  const midY = (fromY + toY) / 2;
  return 'M ' + fromX + ' ' + fromY + ' L ' + fromX + ' ' + midY + ' L ' + toX + ' ' + midY + ' L ' + toX + ' ' + toY;
}

function drawAvatar(g, p, cx, cy, r) {
  // Circle background (fallback colour)
  g.appendChild(svgEl('circle', { cx: cx, cy: cy, r: r, class: 'avatar-bg' }));
  if (p && p.avatar && isSafeImageSrc(p.avatar)) {
    const img = svgEl('image', {
      x: cx - r, y: cy - r, width: r * 2, height: r * 2,
      href: p.avatar, 'clip-path': 'url(#avatar-clip)',
      preserveAspectRatio: 'xMidYMid slice',
    });
    img.setAttributeNS('http://www.w3.org/1999/xlink', 'href', p.avatar);
    g.appendChild(img);
  } else if (p && p.initials) {
    const t = svgEl('text', { x: cx, y: cy + 4, class: 'avatar-initials', 'text-anchor': 'middle' });
    t.textContent = p.initials;
    g.appendChild(t);
  }
}

function drawNode(node) {
  if (node.kind === 'chip') return drawChip(node);
  const g = svgEl('g', { class: 'card-group', 'data-kind': node.kind });
  const w = node.w, h = node.h, x = node.x, y = node.y;
  const rx = 6;
  const bgClass = 'card-bg' + (node.kind === 'focus' ? ' focus' : '') + (node.kind === 'ancestor' ? ' ancestor' : '') + (node.kind === 'sibling' ? ' sibling' : '');
  g.appendChild(svgEl('rect', { x: x, y: y, rx: rx, ry: rx, width: w, height: h, class: bgClass }));
  const p = people.get(node.personId);
  // gender accent (left bar)
  if (p) {
    const sex = (p.sex || '').toLowerCase();
    const cls = sex === 'm' ? 'male' : sex === 'f' ? 'female' : 'unknown';
    g.appendChild(svgEl('line', { x1: x + 1.5, y1: y + rx, x2: x + 1.5, y2: y + h - rx, class: 'card-accent ' + cls }));
  }
  // avatar left-side
  const avatarR = node.kind === 'focus' ? 20 : (node.kind === 'sibling' ? 14 : 16);
  const avatarCx = x + avatarR + 8;
  const avatarCy = y + h / 2;
  drawAvatar(g, p, avatarCx, avatarCy, avatarR);
  // name + years block to the right of avatar
  const textLeft = avatarCx + avatarR + 8;
  const textRight = x + w - 8;
  const textCenter = (textLeft + textRight) / 2;
  const lines = (p && p.name_lines) || [(p && p.display_name) || node.personId];
  const nameClass = 'label-name' + (node.kind === 'focus' ? ' focus' : '') + (node.kind === 'sibling' ? ' sibling' : '');
  const hasYears = personYears(p) && node.kind !== 'sibling';
  const blockH = lines.length * 14 + (hasYears ? 14 : 0);
  let curY = y + (h - blockH) / 2 + 10;
  lines.forEach((line, i) => {
    const t = svgEl('text', { x: textCenter, y: curY, class: nameClass, 'text-anchor': 'middle' });
    t.textContent = line;
    g.appendChild(t);
    curY += 14;
  });
  if (hasYears) {
    const t = svgEl('text', { x: textCenter, y: curY + 1, class: 'label-year', 'text-anchor': 'middle' });
    t.textContent = personYears(p);
    g.appendChild(t);
  }
  // click hit area
  const hit = svgEl('rect', { x: x, y: y, width: w, height: h, class: 'card-hit' });
  if (node.kind !== 'focus') {
    hit.addEventListener('click', () => setFocus(node.personId));
  } else {
    hit.addEventListener('click', () => showDetail(node.personId));
  }
  g.appendChild(hit);
  return g;
}

function drawChip(node) {
  const g = svgEl('g', { class: 'chip' });
  g.appendChild(svgEl('rect', { x: node.x, y: node.y, rx: 12, ry: 12, width: node.w, height: node.h, class: 'chip-bg' }));
  const t = svgEl('text', { x: node.x + node.w / 2, y: node.y + node.h / 2 + 4, class: 'chip-label', 'text-anchor': 'middle' });
  t.textContent = '+ ' + node.count + (node.count === 1 ? ' descendant' : ' descendants');
  g.appendChild(t);
  g.addEventListener('click', () => {
    expandedChildren.add(node.parentId);
    persistExpanded();
    renderScene(true);
  });
  return g;
}

function persistExpanded() {
  try { localStorage.setItem(EXPAND_STORAGE, JSON.stringify([...expandedChildren])); } catch {}
}

function drawScene(scene) {
  viewport.textContent = '';
  // Draw edges first (behind)
  for (const e of scene.edges) {
    let fx, fy, tx, ty;
    if (e.kind === 'spouse') {
      fx = e.from.x + e.from.w; fy = e.from.y + e.from.h / 2;
      tx = e.to.x;              ty = e.to.y + e.to.h / 2;
      viewport.appendChild(svgEl('path', { class: 'spouse-pair', d: 'M ' + fx + ' ' + fy + ' L ' + tx + ' ' + ty }));
    } else {
      // parent (bottom center) -> child (top center)
      fx = e.from.x + e.from.w / 2; fy = e.from.y + e.from.h;
      tx = e.to.x + e.to.w / 2;     ty = e.to.y;
      viewport.appendChild(svgEl('path', { class: 'edge', d: orthogonalPath(fx, fy, tx, ty, 'parent-child') }));
    }
  }
  // Draw nodes on top, in stable order
  for (const id of scene.order) {
    viewport.appendChild(drawNode(scene.nodes.get(id)));
  }
}

function fitSceneToViewport(animated) {
  const r = svg.getBoundingClientRect();
  if (!currentScene || r.width < 10 || r.height < 10) return;
  const pad = 48;
  const bw = currentScene.bbox.width + pad * 2;
  const bh = currentScene.bbox.height + pad * 2;
  svg.setAttribute('viewBox', '0 0 ' + r.width + ' ' + r.height);
  const sx = r.width / bw;
  const sy = r.height / bh;
  // Don't shrink below 0.7 — beyond that text becomes unreadable.
  const fitScale = Math.min(sx, sy, 1.1);
  const newScale = Math.max(fitScale, 0.7);
  const newTx = (r.width - currentScene.bbox.width * newScale) / 2 - currentScene.bbox.minX * newScale;
  const newTy = (r.height - currentScene.bbox.height * newScale) / 2 - currentScene.bbox.minY * newScale;
  if (!animated) viewport.classList.add('no-anim');
  viewScale = newScale;
  viewTranslateX = newTx;
  viewTranslateY = newTy;
  applyViewport();
  if (!animated) requestAnimationFrame(() => viewport.classList.remove('no-anim'));
}

function applyViewport() {
  viewport.setAttribute('transform', 'translate(' + viewTranslateX + ' ' + viewTranslateY + ') scale(' + viewScale + ')');
}

function renderScene(skipFit) {
  if (!focusId) return;
  currentScene = layoutScene(computeScene(focusId));
  drawScene(currentScene);
  renderBreadcrumb();
  renderPeopleList();
  if (!skipFit) fitSceneToViewport(true);
}

// ---- Overview mode: full-tree server-computed layout ----

function renderOverview(skipFit) {
  const ov = data.overview;
  if (!ov) return;
  viewport.textContent = '';
  const ox = ov.offsetX || 0, oy = ov.offsetY || 0;
  // edges first
  for (const e of ov.edges) {
    const fromN = ov.nodes.find(n => n.person_id === e.from);
    const toN = ov.nodes.find(n => n.person_id === e.to && (e.kind === 'spouse' ? n.kind === 'spouse' && n.spouse_of === e.from : n.person_id === e.to));
    if (!fromN || !toN) continue;
    if (e.kind === 'spouse') {
      const fx = fromN.x + ox + fromN.w;
      const fy = fromN.y + oy + fromN.h / 2;
      const tx = toN.x + ox;
      const ty = toN.y + oy + toN.h / 2;
      viewport.appendChild(svgEl('path', { class: 'spouse-pair', d: 'M ' + fx + ' ' + fy + ' L ' + tx + ' ' + ty }));
    } else {
      const fx = fromN.x + ox + fromN.w / 2;
      const fy = fromN.y + oy + fromN.h;
      const tx = toN.x + ox + toN.w / 2;
      const ty = toN.y + oy;
      viewport.appendChild(svgEl('path', { class: 'edge', d: orthogonalPath(fx, fy, tx, ty, 'parent-child') }));
    }
  }
  // nodes
  for (const n of ov.nodes) {
    const g = svgEl('g', { class: 'card-group', 'data-kind': 'overview' });
    const x = n.x + ox, y = n.y + oy;
    const bgClass = 'ov-card-bg' + (n.person_id === focusId ? ' active' : '') + (n.kind === 'spouse' ? ' spouse' : '');
    g.appendChild(svgEl('rect', { x: x, y: y, rx: 5, ry: 5, width: n.w, height: n.h, class: bgClass }));
    const p = people.get(n.person_id);
    // compact avatar (circle)
    const avatarR = 13;
    drawAvatar(g, p, x + 6 + avatarR, y + n.h / 2, avatarR);
    // text
    const textLeft = x + 6 + avatarR * 2 + 6;
    const textRight = x + n.w - 4;
    const textCenter = (textLeft + textRight) / 2;
    const nameShort = (p && p.short_name) || (p && p.display_name) || n.person_id;
    const tName = svgEl('text', { x: textCenter, y: y + (p && p.years ? 24 : n.h / 2 + 4), class: 'ov-name', 'text-anchor': 'middle' });
    tName.textContent = nameShort;
    g.appendChild(tName);
    if (p && p.years) {
      const tYr = svgEl('text', { x: textCenter, y: y + 40, class: 'ov-year', 'text-anchor': 'middle' });
      tYr.textContent = p.years;
      g.appendChild(tYr);
    }
    const hit = svgEl('rect', { x: x, y: y, width: n.w, height: n.h, class: 'card-hit' });
    hit.addEventListener('click', () => { setFocus(n.person_id); setMode('focus'); });
    g.appendChild(hit);
    viewport.appendChild(g);
  }
  currentScene = { bbox: { minX: ox, minY: oy, maxX: ox + (ov.width - 2 * 40), maxY: oy + (ov.height - 2 * 40), width: ov.width - 80, height: ov.height - 80 } };
  if (!skipFit) fitOverviewToFocus();
}

function fitOverviewToFocus() {
  // Open overview at a readable scale (1.0), centered on current focus.
  const r = svg.getBoundingClientRect();
  if (r.width < 10 || r.height < 10) return;
  svg.setAttribute('viewBox', '0 0 ' + r.width + ' ' + r.height);
  const ov = data.overview;
  const focus = ov.nodes.find(n => n.person_id === focusId);
  viewScale = 1.0;
  if (focus) {
    const cx = focus.x + (ov.offsetX || 0) + focus.w / 2;
    const cy = focus.y + (ov.offsetY || 0) + focus.h / 2;
    viewTranslateX = r.width / 2 - cx * viewScale;
    viewTranslateY = r.height / 2 - cy * viewScale;
  } else {
    viewTranslateX = 40; viewTranslateY = 40;
  }
  viewport.classList.add('no-anim');
  applyViewport();
  requestAnimationFrame(() => viewport.classList.remove('no-anim'));
}

// ---- Pagination mode: anchor + N generations down ----

let paginationAnchor = null;
let paginationDepth = 3;

function computePaginationScene(anchorId, depth) {
  // Build a top-down descendancy tree of up to `depth` generations rooted at anchor.
  // Similar to computeScene for children, but nested.
  const scene = { nodes: new Map(), edges: [], order: [] };
  const put = (n) => { scene.nodes.set(n.id, n); scene.order.push(n.id); return n; };
  const visited = new Set();

  function walk(pid, level) {
    if (visited.has(pid) || level > depth) return;
    visited.add(pid);
    const nodeId = 'p:' + pid + ':' + level;
    put({ id: nodeId, kind: (pid === anchorId ? 'focus' : (level === 0 ? 'ancestor' : 'child')), personId: pid, row: level });
    // spouses of this person on the same row
    for (const s of spousesOf(pid)) {
      if (!visited.has(s.spouseId)) {
        const sid = 'p:' + s.spouseId + ':spouse:' + level + ':' + pid;
        visited.add(s.spouseId);
        put({ id: sid, kind: 'spouse', personId: s.spouseId, row: level, familyId: s.familyId, spouse_of: pid });
        scene.edges.push({ kind: 'spouse', fromId: nodeId, toId: sid });
      }
    }
    if (level >= depth) return;
    for (const { childId } of childrenOf(pid)) {
      walk(childId, level + 1);
      scene.edges.push({ kind: 'parent-child', fromId: nodeId, toId: 'p:' + childId + ':' + (level + 1) });
    }
  }

  walk(anchorId, 0);
  return scene;
}

function layoutPaginationScene(scene) {
  // Assign sizes: smaller cards than focus-mode since we show more.
  const CW = 170, CH = 68, GEN = 110, SIB = 20, SP = 14;
  for (const n of scene.nodes.values()) { n.w = CW; n.h = CH; }

  // Group by row
  const byRow = new Map();
  for (const id of scene.order) {
    const n = scene.nodes.get(id);
    if (!byRow.has(n.row)) byRow.set(n.row, []);
    byRow.get(n.row).push(n);
  }

  // For each parent (from parent-child edges), stack their descendants under them.
  // We'll use a simple subtree-width based layout bottom-up:
  const childrenMap = new Map(); // nodeId -> [childNodeId]
  for (const e of scene.edges) {
    if (e.kind !== 'parent-child') continue;
    if (!childrenMap.has(e.fromId)) childrenMap.set(e.fromId, []);
    childrenMap.get(e.fromId).push(e.toId);
  }
  // spouse map to include in "own row width"
  const spouseMap = new Map();
  for (const e of scene.edges) {
    if (e.kind !== 'spouse') continue;
    if (!spouseMap.has(e.fromId)) spouseMap.set(e.fromId, []);
    spouseMap.get(e.fromId).push(e.toId);
  }

  const widths = new Map();
  function measure(id) {
    if (widths.has(id)) return widths.get(id);
    const kids = childrenMap.get(id) || [];
    const sps = spouseMap.get(id) || [];
    const own = CW + sps.length * (CW + SP);
    let w;
    if (!kids.length) w = own;
    else {
      const kidsW = kids.reduce((s, k) => s + measure(k), 0) + SIB * Math.max(0, kids.length - 1);
      w = Math.max(own, kidsW);
    }
    widths.set(id, w);
    return w;
  }

  // Find root(s) — nodes that are not targets of parent-child (but not spouses) — anchor is row 0 non-spouse.
  const anchors = [...scene.nodes.values()].filter(n => n.row === 0 && n.kind !== 'spouse').map(n => n.id);
  anchors.forEach(measure);

  function place(id, left, top) {
    const n = scene.nodes.get(id);
    const sps = spouseMap.get(id) || [];
    const own = CW + sps.length * (CW + SP);
    const w = widths.get(id);
    const rowLeft = left + (w - own) / 2;
    n.x = rowLeft; n.y = top;
    let cx = rowLeft + CW;
    for (const sid of sps) {
      cx += SP;
      const sn = scene.nodes.get(sid);
      sn.x = cx; sn.y = top;
      cx += CW;
    }
    const kids = childrenMap.get(id) || [];
    if (kids.length) {
      const kidsW = kids.reduce((s, k) => s + widths.get(k), 0) + SIB * Math.max(0, kids.length - 1);
      let cursor = left + (w - kidsW) / 2;
      for (const kid of kids) {
        place(kid, cursor, top + GEN);
        cursor += widths.get(kid) + SIB;
      }
    }
  }

  let cursor = 0;
  for (const aid of anchors) {
    place(aid, cursor, 0);
    cursor += widths.get(aid) + 40;
  }

  // bbox
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const n of scene.nodes.values()) {
    minX = Math.min(minX, n.x);
    minY = Math.min(minY, n.y);
    maxX = Math.max(maxX, n.x + n.w);
    maxY = Math.max(maxY, n.y + n.h);
  }
  scene.bbox = { minX, minY, maxX, maxY, width: maxX - minX, height: maxY - minY };
  return scene;
}

function renderPagination(skipFit) {
  if (!paginationAnchor) paginationAnchor = focusId || data.defaultFocus;
  const scene = layoutPaginationScene(computePaginationScene(paginationAnchor, paginationDepth));
  viewport.textContent = '';
  // edges
  for (const e of scene.edges) {
    const from = scene.nodes.get(e.fromId);
    const to = scene.nodes.get(e.toId);
    if (!from || !to) continue;
    if (e.kind === 'spouse') {
      const fx = from.x + from.w, fy = from.y + from.h / 2;
      const tx = to.x, ty = to.y + to.h / 2;
      viewport.appendChild(svgEl('path', { class: 'spouse-pair', d: 'M ' + fx + ' ' + fy + ' L ' + tx + ' ' + ty }));
    } else {
      const fx = from.x + from.w / 2, fy = from.y + from.h;
      const tx = to.x + to.w / 2, ty = to.y;
      viewport.appendChild(svgEl('path', { class: 'edge', d: orthogonalPath(fx, fy, tx, ty, 'parent-child') }));
    }
  }
  // nodes — reuse drawNode style
  for (const id of scene.order) {
    const n = scene.nodes.get(id);
    viewport.appendChild(drawNode(n));
  }
  currentScene = scene;
  if (!skipFit) fitPaginationToAnchor(scene);
}

function fitPaginationToAnchor(scene) {
  const r = svg.getBoundingClientRect();
  if (r.width < 10 || r.height < 10) return;
  svg.setAttribute('viewBox', '0 0 ' + r.width + ' ' + r.height);
  // Decide scale: if subtree fits, fit it; otherwise default 1.0 with anchor on top-center.
  const bw = scene.bbox.width + 80, bh = scene.bbox.height + 80;
  const fitScale = Math.min(r.width / bw, r.height / bh, 1.0);
  viewScale = Math.max(fitScale, 0.5);
  // Anchor = row 0 first non-spouse
  const anchor = [...scene.nodes.values()].find(n => n.row === 0 && n.kind !== 'spouse');
  if (anchor) {
    const cx = anchor.x + anchor.w / 2;
    const cy = anchor.y + anchor.h / 2;
    viewTranslateX = r.width / 2 - cx * viewScale;
    viewTranslateY = Math.min(r.height * 0.25 - cy * viewScale, 40);
  } else {
    viewTranslateX = 40; viewTranslateY = 40;
  }
  viewport.classList.add('no-anim');
  applyViewport();
  requestAnimationFrame(() => viewport.classList.remove('no-anim'));
}

// ---- Mode switching ----
let currentMode = 'overview';

function setMode(mode, opts) {
  opts = opts || {};
  if (!['overview', 'pagination', 'focus'].includes(mode)) mode = 'overview';
  currentMode = mode;
  try { localStorage.setItem('genealogy.mode.v1', mode); } catch {}
  // update UI tabs
  for (const btn of document.querySelectorAll('.mode-tabs button')) {
    btn.classList.toggle('active', btn.dataset.mode === mode);
  }
  document.getElementById('pagination-controls').hidden = mode !== 'pagination';
  // update URL query
  if (!opts.skipUrl) {
    const url = new URL(window.location.href);
    if (mode === 'overview') url.searchParams.delete('mode'); else url.searchParams.set('mode', mode);
    history.replaceState(null, '', url.pathname + url.search + url.hash);
  }
  render();
}

function render(skipFit) {
  if (currentMode === 'overview') renderOverview(skipFit);
  else if (currentMode === 'pagination') { paginationAnchor = focusId || data.defaultFocus; renderPagination(skipFit); }
  else renderScene(skipFit);
  renderBreadcrumb();
  renderPeopleList();
}

// ---- Breadcrumb: ancestry path from root to current focus ----
function ancestryPath(pid) {
  const path = [];
  const seen = new Set();
  let cur = pid;
  while (cur && !seen.has(cur)) {
    seen.add(cur);
    path.push(cur);
    const parentFid = parentFamily[cur];
    if (!parentFid) break;
    const fam = families.get(parentFid);
    if (!fam) break;
    const up = fam.spouses.find(s => s);
    if (!up) break;
    cur = up;
  }
  return path.reverse();
}

function renderBreadcrumb() {
  breadcrumb.textContent = '';
  const path = ancestryPath(focusId);
  path.forEach((pid, i) => {
    if (i > 0) {
      const sep = htmlEl('span', { class: 'sep' });
      sep.textContent = '>';
      breadcrumb.appendChild(sep);
    }
    const p = people.get(pid);
    const crumb = htmlEl('span', { class: 'crumb' + (pid === focusId ? ' current' : '') });
    crumb.textContent = p ? p.display_name : pid;
    if (pid !== focusId) crumb.addEventListener('click', () => setFocus(pid));
    breadcrumb.appendChild(crumb);
  });
}

// ---- People list (sidebar) grouped by root ancestor ----
function renderPeopleList() {
  peopleList.textContent = '';
  const filter = searchInput.value.trim().toLowerCase();
  const byRoot = new Map();
  for (const p of data.persons) {
    if (filter && !p.display_name.toLowerCase().includes(filter)) continue;
    const rid = personRoots[p.person_id] || p.person_id;
    if (!byRoot.has(rid)) byRoot.set(rid, []);
    byRoot.get(rid).push(p);
  }
  const orderedRoots = rootPersons.filter(r => byRoot.has(r));
  for (const rid of byRoot.keys()) if (!orderedRoots.includes(rid)) orderedRoots.push(rid);
  for (const rid of orderedRoots) {
    const group = byRoot.get(rid);
    group.sort((a, b) => a.display_name.localeCompare(b.display_name, 'ru'));
    const section = htmlEl('div', { class: 'root-group' });
    const title = htmlEl('h3');
    const rootPerson = people.get(rid);
    title.textContent = rootPerson ? rootPerson.display_name : rid;
    section.appendChild(title);
    for (const p of group) {
      const item = htmlEl('div', { class: 'person-item' + (p.person_id === focusId ? ' current' : '') });
      const name = htmlEl('span');
      name.textContent = p.display_name;
      const years = htmlEl('span', { class: 'years' });
      years.textContent = p.years || '';
      item.append(name, years);
      item.addEventListener('click', () => setFocus(p.person_id));
      section.appendChild(item);
    }
    peopleList.appendChild(section);
  }
  if (!peopleList.children.length) {
    const empty = htmlEl('div', { class: 'empty-state' });
    empty.textContent = 'No matches.';
    empty.style.padding = '12px';
    peopleList.appendChild(empty);
  }
}

// ---- Detail panel ----
function showEmptyDetail() {
  detail.replaceChildren();
  if (detailCollapseBtn) detail.appendChild(detailCollapseBtn);
  const e = htmlEl('div', { class: 'empty-state' });
  e.textContent = 'Select a person to view details.';
  detail.appendChild(e);
}
function addDetailRow(label, value) {
  if (!value) return;
  const row = htmlEl('div', { class: 'detail-row' });
  const s = htmlEl('strong');
  s.textContent = label + ':';
  row.append(s, ' ' + value);
  detail.appendChild(row);
}
function showDetail(pid) {
  const p = people.get(pid);
  if (!p) return;
  detail.replaceChildren();
  if (detailCollapseBtn) detail.appendChild(detailCollapseBtn);
  if (bootComplete && mainEl.classList.contains('right-collapsed')) {
    setCollapsed('right', false, { persist: false });
    detailEphemerallyExpanded = true;
  }
  const header = htmlEl('div', { class: 'detail-header' });
  const h = htmlEl('h2');
  h.textContent = p.display_name;
  const meta = htmlEl('div', { class: 'muted' });
  meta.textContent = (p.years || '') + (p.source_visio_id ? ' · ' + p.source_visio_id : '');
  header.append(h, meta);
  detail.appendChild(header);
  addDetailRow('Birth', p.birth);
  addDetailRow('Christening', p.christening);
  addDetailRow('Death', p.death);
  addDetailRow('Notes', p.notes);
  for (const photo of p.media || []) {
    if (!isSafeImageSrc(photo.relative_file_path)) continue;
    const figure = htmlEl('figure');
    const img = htmlEl('img', { class: 'photo' });
    img.setAttribute('src', photo.relative_file_path);
    img.setAttribute('alt', photo.title || p.display_name);
    const cap = htmlEl('figcaption');
    cap.textContent = photo.title || '';
    figure.append(img, cap);
    detail.appendChild(figure);
  }
  detail.classList.add('open');
}

// ---- Focus state & navigation ----
function setFocus(pid, opts) {
  opts = opts || {};
  if (!people.has(pid)) return;
  if (pid === focusId && !opts.force) return;
  if (!opts.fromHistory) {
    historyStack = historyStack.slice(0, historyIndex + 1);
    historyStack.push(pid);
    historyIndex = historyStack.length - 1;
  }
  focusId = pid;
  paginationAnchor = pid;
  try { localStorage.setItem(FOCUS_STORAGE, pid); } catch {}
  if (!opts.skipHash) {
    const url = new URL(window.location.href);
    url.hash = '#' + pid;
    history.replaceState(null, '', url.pathname + url.search + url.hash);
  }
  updateNavButtons();
  showDetail(pid);
  render();
}

function updateNavButtons() {
  document.getElementById('nav-back').disabled = historyIndex <= 0;
  document.getElementById('nav-forward').disabled = historyIndex >= historyStack.length - 1;
}

// ---- Event wiring ----
searchInput.addEventListener('input', () => {
  renderPeopleList();
  const val = searchInput.value.trim().toLowerCase();
  if (!val) return;
  const match = data.persons.find(p => p.display_name.toLowerCase().includes(val));
  if (match) setFocus(match.person_id);
});
document.getElementById('nav-back').addEventListener('click', () => {
  if (historyIndex > 0) { historyIndex--; setFocus(historyStack[historyIndex], { fromHistory: true }); }
});
document.getElementById('nav-forward').addEventListener('click', () => {
  if (historyIndex < historyStack.length - 1) { historyIndex++; setFocus(historyStack[historyIndex], { fromHistory: true }); }
});
document.getElementById('zoom-in').addEventListener('click', () => { viewScale = Math.min(viewScale * 1.2, 3.0); applyViewport(); });
document.getElementById('zoom-out').addEventListener('click', () => { viewScale = Math.max(viewScale / 1.2, 0.2); applyViewport(); });
document.getElementById('reset-view').addEventListener('click', () => fitSceneToViewport(true));
document.getElementById('drawer-toggle').addEventListener('click', () => sidebar.classList.toggle('open'));
document.getElementById('detail-toggle').addEventListener('click', () => detail.classList.toggle('open'));

function updateCollapseButton(side, collapsed) {
  const btn = side === 'left' ? sidebarCollapseBtn : detailCollapseBtn;
  if (!btn) return;
  if (side === 'left') {
    btn.innerHTML = collapsed ? '&#8250;' : '&#8249;';
    btn.title = collapsed ? 'Expand sidebar' : 'Collapse sidebar';
  } else {
    btn.innerHTML = collapsed ? '&#8249;' : '&#8250;';
    btn.title = collapsed ? 'Expand details' : 'Collapse details';
  }
  btn.setAttribute('aria-label', btn.title);
  btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
}
function setCollapsed(side, collapsed, opts) {
  opts = opts || {};
  const persist = opts.persist !== false;
  const cls = side === 'left' ? 'left-collapsed' : 'right-collapsed';
  mainEl.classList.toggle(cls, collapsed);
  if (persist) {
    const key = side === 'left' ? SIDEBAR_COLLAPSED_KEY : DETAIL_COLLAPSED_KEY;
    try { localStorage.setItem(key, collapsed ? '1' : '0'); } catch {}
  }
  updateCollapseButton(side, collapsed);
  let fitted = false;
  const onEnd = (ev) => {
    if (ev.target !== mainEl || ev.propertyName !== 'grid-template-columns') return;
    mainEl.removeEventListener('transitionend', onEnd);
    if (fitted) return;
    fitted = true;
    fitSceneToViewport(false);
  };
  mainEl.addEventListener('transitionend', onEnd);
  setTimeout(() => {
    if (fitted) return;
    fitted = true;
    mainEl.removeEventListener('transitionend', onEnd);
    fitSceneToViewport(false);
  }, 320);
}
if (sidebarCollapseBtn) {
  sidebarCollapseBtn.addEventListener('click', () => {
    setCollapsed('left', !mainEl.classList.contains('left-collapsed'));
  });
}
if (detailCollapseBtn) {
  detailCollapseBtn.addEventListener('click', () => {
    const willCollapse = !mainEl.classList.contains('right-collapsed');
    setCollapsed('right', willCollapse);
    detailEphemerallyExpanded = false;
  });
}
try {
  if (localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === '1') {
    mainEl.classList.add('left-collapsed');
    updateCollapseButton('left', true);
  }
  if (localStorage.getItem(DETAIL_COLLAPSED_KEY) === '1') {
    mainEl.classList.add('right-collapsed');
    updateCollapseButton('right', true);
  }
} catch {}

window.addEventListener('hashchange', () => {
  const pid = window.location.hash.replace(/^#/, '');
  if (pid && people.has(pid) && pid !== focusId) setFocus(pid, { skipHash: true });
});
window.addEventListener('keydown', (e) => {
  if (e.target === searchInput) return;
  if (e.key === 'ArrowLeft' && historyIndex > 0) { historyIndex--; setFocus(historyStack[historyIndex], { fromHistory: true }); }
  else if (e.key === 'ArrowRight' && historyIndex < historyStack.length - 1) { historyIndex++; setFocus(historyStack[historyIndex], { fromHistory: true }); }
});

// ---- Pan & zoom ----
function clientToSvg(cx, cy) {
  const r = svg.getBoundingClientRect();
  return { x: cx - r.left, y: cy - r.top };
}
svg.addEventListener('mousedown', (e) => {
  dragState = { sx: e.clientX, sy: e.clientY, tx: viewTranslateX, ty: viewTranslateY };
  svg.classList.add('dragging');
  viewport.classList.add('no-anim');
});
window.addEventListener('mousemove', (e) => {
  if (!dragState) return;
  viewTranslateX = dragState.tx + (e.clientX - dragState.sx);
  viewTranslateY = dragState.ty + (e.clientY - dragState.sy);
  applyViewport();
});
window.addEventListener('mouseup', () => {
  if (!dragState) return;
  dragState = null;
  svg.classList.remove('dragging');
  requestAnimationFrame(() => viewport.classList.remove('no-anim'));
});
svg.addEventListener('touchstart', (e) => {
  if (e.touches.length !== 1) { dragState = null; svg.classList.remove('dragging'); return; }
  const t = e.touches[0];
  dragState = { sx: t.clientX, sy: t.clientY, tx: viewTranslateX, ty: viewTranslateY, touch: true, panning: false };
}, { passive: true });
window.addEventListener('touchmove', (e) => {
  if (!dragState || !dragState.touch) return;
  if (e.touches.length !== 1) { dragState = null; svg.classList.remove('dragging'); return; }
  const t = e.touches[0];
  const dx = t.clientX - dragState.sx;
  const dy = t.clientY - dragState.sy;
  if (!dragState.panning && Math.hypot(dx, dy) < 6) return;
  if (!dragState.panning) { dragState.panning = true; svg.classList.add('dragging'); viewport.classList.add('no-anim'); }
  if (e.cancelable) e.preventDefault();
  viewTranslateX = dragState.tx + dx;
  viewTranslateY = dragState.ty + dy;
  applyViewport();
}, { passive: false });
window.addEventListener('touchend', () => {
  if (!dragState) return;
  dragState = null;
  svg.classList.remove('dragging');
  requestAnimationFrame(() => viewport.classList.remove('no-anim'));
});
window.addEventListener('touchcancel', () => {
  dragState = null;
  svg.classList.remove('dragging');
  viewport.classList.remove('no-anim');
});
svg.addEventListener('wheel', (e) => {
  e.preventDefault();
  const pt = clientToSvg(e.clientX, e.clientY);
  const wx = (pt.x - viewTranslateX) / viewScale;
  const wy = (pt.y - viewTranslateY) / viewScale;
  const next = e.deltaY < 0 ? Math.min(viewScale * 1.1, 3.0) : Math.max(viewScale / 1.1, 0.18);
  viewTranslateX = pt.x - wx * next;
  viewTranslateY = pt.y - wy * next;
  viewScale = next;
  viewport.classList.add('no-anim');
  applyViewport();
  requestAnimationFrame(() => viewport.classList.remove('no-anim'));
}, { passive: false });

window.addEventListener('resize', () => fitSceneToViewport(false));

// ---- Mode tabs + pagination controls wiring ----
for (const btn of document.querySelectorAll('.mode-tabs button')) {
  btn.addEventListener('click', () => setMode(btn.dataset.mode));
}
document.getElementById('gen-depth').addEventListener('change', (e) => {
  paginationDepth = parseInt(e.target.value, 10) || 3;
  if (currentMode === 'pagination') renderPagination(false);
});
document.getElementById('gen-up').addEventListener('click', () => {
  const pf = parentFamily[paginationAnchor];
  if (!pf) return;
  const fam = families.get(pf);
  if (!fam) return;
  const upId = fam.spouses[0];
  if (upId) { paginationAnchor = upId; if (currentMode === 'pagination') renderPagination(false); }
});
document.getElementById('gen-down').addEventListener('click', () => {
  const kids = childrenOf(paginationAnchor);
  if (!kids.length) return;
  paginationAnchor = kids[0].childId;
  if (currentMode === 'pagination') renderPagination(false);
});

// ---- Boot ----
function initialFocus() {
  const hashPid = window.location.hash.replace(/^#/, '');
  if (hashPid && people.has(hashPid)) return hashPid;
  try {
    const saved = localStorage.getItem(FOCUS_STORAGE);
    if (saved && people.has(saved)) return saved;
  } catch {}
  return data.defaultFocus || (data.persons[0] && data.persons[0].person_id);
}
function initialMode() {
  const url = new URL(window.location.href);
  const q = url.searchParams.get('mode');
  if (q && ['overview', 'pagination', 'focus'].includes(q)) return q;
  try {
    const saved = localStorage.getItem('genealogy.mode.v1');
    if (saved && ['overview', 'pagination', 'focus'].includes(saved)) return saved;
  } catch {}
  return 'overview';
}

const startPid = initialFocus();
if (startPid) {
  focusId = startPid;
  paginationAnchor = startPid;
  historyStack = [startPid];
  historyIndex = 0;
  try { localStorage.setItem(FOCUS_STORAGE, startPid); } catch {}
  updateNavButtons();
  showDetail(startPid);
  setMode(initialMode(), { skipUrl: false });
  requestAnimationFrame(() => { fitSceneToViewport(false); bootComplete = true; });
} else {
  showEmptyDetail();
  const empty = htmlEl('div', { class: 'empty-state' });
  empty.textContent = 'No data.';
  breadcrumb.appendChild(empty);
}

})();
</script>
</body>
</html>
"""
