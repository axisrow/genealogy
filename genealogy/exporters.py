from __future__ import annotations

import json
import sqlite3
from collections import defaultdict, deque
from pathlib import Path
import re
from xml.sax.saxutils import escape

from .db import connect


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

    lines = ["0 HEAD", "1 SOUR genealogy-converter", "1 GEDC", "2 VERS 5.5.1", "2 FORM LINEAGE-LINKED", "1 CHAR UTF-8"]
    for person in persons:
        lines.append(f"0 @{person['person_id']}@ INDI")
        lines.append(f"1 NAME {person['display_name']}")
        if person["birth"]:
            lines.extend(["1 BIRT", f"2 DATE {person['birth']}"])
        if person["christening"]:
            lines.extend(["1 CHR", f"2 DATE {person['christening']}"])
        if person["death"]:
            lines.extend(["1 DEAT", f"2 DATE {person['death']}"])
        if person["notes"]:
            lines.append(f"1 NOTE {person['notes']}")
        for item in media_by_person[person["person_id"]]:
            lines.extend(["1 OBJE", f"2 FILE {item['relative_file_path']}"])
            if item["title"]:
                lines.append(f"2 TITL {item['title']}")

    for family in families:
        lines.append(f"0 @{family['family_id']}@ FAM")
        if family["spouse1_person_id"]:
            lines.append(f"1 HUSB @{family['spouse1_person_id']}@")
        if family["spouse2_person_id"]:
            lines.append(f"1 WIFE @{family['spouse2_person_id']}@")
        for child_id in children_by_family[family["family_id"]]:
            lines.append(f"1 CHIL @{child_id}@")
        if family["original_combined_text"]:
            lines.append(f"1 NOTE Source text: {family['original_combined_text']}")
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
    html = HTML_TEMPLATE.replace("__DATA__", json.dumps(payload, ensure_ascii=False))
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


FAMILY_WIDTH = 260
FAMILY_HEIGHT = 100
PERSON_WIDTH = 144
PERSON_HEIGHT = 40
ROOT_GAP = 96
CHILD_GAP = 30
DESCENDANT_GAP = 36
CHILD_ROW_GAP = 56
FAMILY_STACK_GAP = 70
CANVAS_PADDING = 56


def _build_html_payload(persons: list[dict], families: list[dict], children: list[dict], media: list[dict]) -> dict:
    person_by_id = {person["person_id"]: person for person in persons}
    family_by_id = {family["family_id"]: family for family in families}
    children_by_family: dict[str, list[str]] = defaultdict(list)
    families_by_person: dict[str, list[str]] = defaultdict(list)
    parent_family_by_child: dict[str, list[str]] = defaultdict(list)
    media_by_person: dict[str, list[dict]] = defaultdict(list)

    for item in media:
        media_by_person[item["person_id"]].append(item)
    for family in families:
        for person_id in (family["spouse1_person_id"], family["spouse2_person_id"]):
            if person_id:
                families_by_person[person_id].append(family["family_id"])
    for link in children:
        children_by_family[link["family_id"]].append(link["child_person_id"])
        parent_family_by_child[link["child_person_id"]].append(link["family_id"])

    roots = [
        family["family_id"]
        for family in families
        if all(
            spouse_id not in parent_family_by_child
            for spouse_id in (family["spouse1_person_id"], family["spouse2_person_id"])
            if spouse_id
        )
    ]
    if not roots:
        roots = [family["family_id"] for family in families]

    measure_cache: dict[str, dict] = {}

    def descendants_for_child(parent_family_id: str, child_person_id: str) -> list[str]:
        family_ids = []
        for family_id in families_by_person.get(child_person_id, []):
            if family_id != parent_family_id:
                family_ids.append(family_id)
        return sorted(dict.fromkeys(family_ids))

    def measure_family(family_id: str, stack: tuple[str, ...]) -> dict:
        if family_id in measure_cache:
            return measure_cache[family_id]
        if family_id in stack:
            return {
                "width": FAMILY_WIDTH,
                "height": FAMILY_HEIGHT,
                "children_width": 0,
                "max_child_block_height": 0,
                "child_measures": [],
            }

        child_measures = []
        max_child_block_height = 0
        total_children_width = 0
        for child_person_id in children_by_family.get(family_id, []):
            descendant_ids = descendants_for_child(family_id, child_person_id)
            descendant_measures = [measure_family(descendant_id, stack + (family_id,)) for descendant_id in descendant_ids]
            descendants_width = 0
            descendants_height = 0
            if descendant_measures:
                descendants_width = sum(item["width"] for item in descendant_measures) + DESCENDANT_GAP * (len(descendant_measures) - 1)
                descendants_height = max(item["height"] for item in descendant_measures)
            span_width = max(PERSON_WIDTH, descendants_width)
            span_height = PERSON_HEIGHT + (FAMILY_STACK_GAP + descendants_height if descendant_measures else 0)
            child_measure = {
                "person_id": child_person_id,
                "descendant_ids": descendant_ids,
                "descendant_measures": descendant_measures,
                "span_width": span_width,
                "span_height": span_height,
                "descendants_width": descendants_width,
                "descendants_height": descendants_height,
            }
            child_measures.append(child_measure)
            total_children_width += span_width
            max_child_block_height = max(max_child_block_height, span_height)
        if child_measures:
            total_children_width += CHILD_GAP * (len(child_measures) - 1)
        measurement = {
            "width": max(FAMILY_WIDTH, total_children_width),
            "height": FAMILY_HEIGHT + (CHILD_ROW_GAP + max_child_block_height if child_measures else 0),
            "children_width": total_children_width,
            "max_child_block_height": max_child_block_height,
            "child_measures": child_measures,
        }
        measure_cache[family_id] = measurement
        return measurement

    family_nodes: list[dict] = []
    child_nodes: list[dict] = []
    edges: list[dict] = []
    focus_nodes: dict[str, list[str]] = defaultdict(list)
    rendered_families: set[str] = set()
    root_measures = [measure_family(root_id, ()) for root_id in roots]
    total_width = sum(item["width"] for item in root_measures) + ROOT_GAP * max(0, len(root_measures) - 1)
    max_height = max((item["height"] for item in root_measures), default=FAMILY_HEIGHT)

    def person_card_data(person_id: str) -> dict:
        person = person_by_id[person_id]
        return {
            "person_id": person_id,
            "name": person["display_name"],
            "name_lines": _wrap_label(person["display_name"], 16, 2),
            "years": _short_year_label(person),
        }

    def family_card_data(family_id: str, x: float, y: float) -> dict:
        family = family_by_id[family_id]
        spouses = [
            person_card_data(person_id)
            for person_id in (family["spouse1_person_id"], family["spouse2_person_id"])
            if person_id and person_id in person_by_id
        ]
        for spouse in spouses:
            focus_nodes[spouse["person_id"]].append(family_id)
        return {
            "family_id": family_id,
            "x": round(x, 2),
            "y": round(y, 2),
            "width": FAMILY_WIDTH,
            "height": FAMILY_HEIGHT,
            "spouses": spouses,
        }

    def layout_family(family_id: str, left: float, top: float, measurement: dict) -> None:
        if family_id in rendered_families:
            return
        rendered_families.add(family_id)
        family_x = left + (measurement["width"] - FAMILY_WIDTH) / 2
        family_nodes.append(family_card_data(family_id, family_x, top))

        if not measurement["child_measures"]:
            return

        family_center_x = family_x + FAMILY_WIDTH / 2
        child_row_y = top + FAMILY_HEIGHT + CHILD_ROW_GAP
        cursor_x = left + (measurement["width"] - measurement["children_width"]) / 2
        for child_measure in measurement["child_measures"]:
            child_x = cursor_x + (child_measure["span_width"] - PERSON_WIDTH) / 2
            child_node_id = f"{family_id}:{child_measure['person_id']}"
            child_nodes.append(
                {
                    "node_id": child_node_id,
                    "family_id": family_id,
                    "x": round(child_x, 2),
                    "y": round(child_row_y, 2),
                    "width": PERSON_WIDTH,
                    "height": PERSON_HEIGHT,
                    **person_card_data(child_measure["person_id"]),
                }
            )
            focus_nodes[child_measure["person_id"]].append(child_node_id)
            child_center_x = child_x + PERSON_WIDTH / 2
            edges.append(
                {
                    "kind": "family-child",
                    "from_x": round(family_center_x, 2),
                    "from_y": round(top + FAMILY_HEIGHT, 2),
                    "to_x": round(child_center_x, 2),
                    "to_y": round(child_row_y, 2),
                }
            )
            if child_measure["descendant_measures"]:
                descendant_total_width = child_measure["descendants_width"]
                descendant_left = cursor_x + (child_measure["span_width"] - descendant_total_width) / 2
                descendant_top = child_row_y + PERSON_HEIGHT + FAMILY_STACK_GAP
                current_left = descendant_left
                for descendant_id, descendant_measure in zip(
                    child_measure["descendant_ids"], child_measure["descendant_measures"], strict=False
                ):
                    descendant_center_x = current_left + descendant_measure["width"] / 2
                    edges.append(
                        {
                            "kind": "child-family",
                            "from_x": round(child_center_x, 2),
                            "from_y": round(child_row_y + PERSON_HEIGHT, 2),
                            "to_x": round(descendant_center_x, 2),
                            "to_y": round(descendant_top, 2),
                        }
                    )
                    layout_family(descendant_id, current_left, descendant_top, descendant_measure)
                    current_left += descendant_measure["width"] + DESCENDANT_GAP
            cursor_x += child_measure["span_width"] + CHILD_GAP

    current_left = CANVAS_PADDING
    for root_id, root_measure in zip(roots, root_measures, strict=False):
        layout_family(root_id, current_left, CANVAS_PADDING, root_measure)
        current_left += root_measure["width"] + ROOT_GAP

    details = []
    for person in persons:
        details.append(
            {
                **person,
                "years": _short_year_label(person),
                "media": media_by_person.get(person["person_id"], []),
                "focus_nodes": focus_nodes.get(person["person_id"], []),
            }
        )

    return {
        "persons": details,
        "familyNodes": family_nodes,
        "childNodes": child_nodes,
        "edges": edges,
        "canvas": {
            "width": round(total_width + CANVAS_PADDING * 2, 2),
            "height": round(max_height + CANVAS_PADDING * 2, 2),
        },
    }


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
:root { color-scheme: light; font-family: Arial, sans-serif; }
body { margin: 0; background: #f5f7fb; color: #1c2430; }
header { padding: 14px 18px; background: #ffffff; border-bottom: 1px solid #d8dee8; display: flex; gap: 12px; align-items: center; }
h1 { font-size: 20px; margin: 0; white-space: nowrap; }
#search { flex: 1; min-width: 180px; max-width: 420px; padding: 10px 12px; border: 1px solid #aeb8c7; border-radius: 6px; font-size: 14px; }
.toolbar { display: inline-flex; gap: 8px; margin-left: auto; }
.icon-btn { width: 32px; height: 32px; border: 1px solid #aeb8c7; border-radius: 6px; background: #fff; cursor: pointer; font-size: 16px; }
main { display: grid; grid-template-columns: minmax(320px, 1fr) 360px; min-height: calc(100vh - 61px); }
#tree-shell { padding: 0; overflow: hidden; position: relative; background: #eef3fb; }
#tree-svg { width: 100%; height: 100%; display: block; cursor: grab; user-select: none; }
#tree-svg.dragging { cursor: grabbing; }
#detail { background: #ffffff; border-left: 1px solid #d8dee8; padding: 20px; overflow: auto; }
.muted { color: #637083; font-size: 13px; }
.detail-header { margin-bottom: 14px; }
.detail-header h2 { margin: 0 0 6px; font-size: 22px; }
.photo { max-width: 100%; border-radius: 6px; border: 1px solid #d8dee8; margin: 10px 0; display: block; }
.family-card { fill: #ffffff; stroke: #8ea2bd; stroke-width: 1.4; }
.family-card.active { stroke: #1d5ea8; stroke-width: 2.2; }
.family-divider { stroke: #d5dde9; stroke-width: 1; }
.person-card { fill: #ffffff; stroke: #adb8c8; stroke-width: 1.2; }
.person-card.active { fill: #edf5ff; stroke: #1d5ea8; stroke-width: 2; }
.edge { fill: none; stroke: #94a4bb; stroke-width: 1.4; }
.label-name { fill: #1b2431; font-size: 13px; font-weight: 600; }
.label-year { fill: #66758a; font-size: 11px; }
.spouse-hit, .child-hit { fill: transparent; cursor: pointer; }
.empty-state { color: #5f6f84; font-size: 14px; padding: 24px; }
@media (max-width: 760px) {
  header { flex-wrap: wrap; }
  #search { width: 100%; max-width: none; flex-basis: 100%; }
  .toolbar { margin-left: 0; }
  main { grid-template-columns: 1fr; }
  #tree-shell { min-height: 62vh; }
  #detail { border-left: 0; border-top: 1px solid #d8dee8; min-height: 30vh; }
}
</style>
</head>
<body>
<header>
  <h1>Genealogy Tree</h1>
  <input id="search" type="search" placeholder="Search people">
  <div class="toolbar">
    <button id="zoom-in" class="icon-btn" type="button" title="Zoom in">+</button>
    <button id="zoom-out" class="icon-btn" type="button" title="Zoom out">-</button>
    <button id="reset-view" class="icon-btn" type="button" title="Reset view">&#8962;</button>
  </div>
</header>
<main>
  <section id="tree-shell">
    <svg id="tree-svg" viewBox="0 0 100 100" aria-label="Genealogy tree">
      <g id="viewport"></g>
    </svg>
  </section>
  <aside id="detail"><div class="empty-state">Select a person to view details.</div></aside>
</main>
<script id="tree-data" type="application/json">__DATA__</script>
<script>
const data = JSON.parse(document.getElementById('tree-data').textContent);
const SVG_NS = 'http://www.w3.org/2000/svg';
const people = new Map(data.persons.map(person => [person.person_id, person]));
const focusNodes = new Map();
for (const person of data.persons) focusNodes.set(person.person_id, person.focus_nodes || []);
const viewport = document.getElementById('viewport');
const svg = document.getElementById('tree-svg');
const detail = document.getElementById('detail');
let selectedPersonId = null;
let scale = 1;
let translateX = 0;
let translateY = 0;
let dragState = null;

function el(name, attrs = {}) {
  const node = document.createElementNS(SVG_NS, name);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
  return node;
}

function updateViewport() {
  viewport.setAttribute('transform', `translate(${translateX} ${translateY}) scale(${scale})`);
}

function fitTree() {
  const width = Math.max(svg.clientWidth || 100, 100);
  const height = Math.max(svg.clientHeight || 100, 100);
  const pad = 24;
  scale = Math.min((width - pad * 2) / data.canvas.width, (height - pad * 2) / data.canvas.height, 1);
  translateX = (width - data.canvas.width * scale) / 2;
  translateY = 18;
  updateViewport();
}

function centerOnPoint(x, y) {
  const width = Math.max(svg.clientWidth || 100, 100);
  const height = Math.max(svg.clientHeight || 100, 100);
  translateX = width / 2 - x * scale;
  translateY = height / 2 - y * scale;
  updateViewport();
}

function showPerson(personId) {
  selectedPersonId = personId;
  const person = people.get(personId);
  if (!person) return;
  detail.innerHTML = `<div class="detail-header"><h2>${person.display_name}</h2><div class="muted">${person.years || ''}${person.source_visio_id ? ' | ' + person.source_visio_id : ''}</div></div>
    ${person.birth ? `<p><strong>Birth:</strong> ${person.birth}</p>` : ''}
    ${person.christening ? `<p><strong>Christening:</strong> ${person.christening}</p>` : ''}
    ${person.death ? `<p><strong>Death:</strong> ${person.death}</p>` : ''}
    ${person.notes ? `<p><strong>Notes:</strong> ${person.notes}</p>` : ''}
    ${(person.media || []).map(photo => `<figure><img class="photo" src="${photo.relative_file_path}" alt="${photo.title || person.display_name}"><figcaption>${photo.title || ''}</figcaption></figure>`).join('')}`;
  renderTree();
}

function drawEdge(edge) {
  const midY = edge.kind === 'family-child' ? edge.from_y + 24 : edge.from_y + 22;
  return el('path', {
    class: 'edge',
    d: `M ${edge.from_x} ${edge.from_y} L ${edge.from_x} ${midY} L ${edge.to_x} ${midY} L ${edge.to_x} ${edge.to_y}`
  });
}

function addTextLines(group, lines, x, startY, className) {
  lines.forEach((line, index) => {
    const text = el('text', { x, y: startY + index * 15, class: className, 'text-anchor': 'middle' });
    text.textContent = line;
    group.appendChild(text);
  });
}

function drawFamily(node) {
  const group = el('g', { 'data-family-id': node.family_id });
  const active = selectedPersonId && node.spouses.some(spouse => spouse.person_id === selectedPersonId);
  group.appendChild(el('rect', {
    x: node.x,
    y: node.y,
    rx: 8,
    ry: 8,
    width: node.width,
    height: node.height,
    class: active ? 'family-card active' : 'family-card'
  }));
  const hasTwo = node.spouses.length === 2;
  if (hasTwo) {
    group.appendChild(el('line', {
      x1: node.x + node.width / 2,
      y1: node.y + 8,
      x2: node.x + node.width / 2,
      y2: node.y + node.height - 8,
      class: 'family-divider'
    }));
  }
  node.spouses.forEach((spouse, index) => {
    const sectionWidth = hasTwo ? node.width / 2 : node.width;
    const left = node.x + (hasTwo ? index * sectionWidth : 0);
    const centerX = left + sectionWidth / 2;
    const lines = spouse.name_lines || [spouse.name];
    addTextLines(group, lines, centerX, node.y + 28, 'label-name');
    if (spouse.years) {
      const years = el('text', { x: centerX, y: node.y + 80, class: 'label-year', 'text-anchor': 'middle' });
      years.textContent = spouse.years;
      group.appendChild(years);
    }
    const hit = el('rect', {
      x: left,
      y: node.y,
      width: sectionWidth,
      height: node.height,
      class: 'spouse-hit'
    });
    hit.addEventListener('click', () => showPerson(spouse.person_id));
    group.appendChild(hit);
  });
  return group;
}

function drawChild(node) {
  const group = el('g', { 'data-node-id': node.node_id });
  const active = selectedPersonId === node.person_id;
  group.appendChild(el('rect', {
    x: node.x,
    y: node.y,
    rx: 8,
    ry: 8,
    width: node.width,
    height: node.height,
    class: active ? 'person-card active' : 'person-card'
  }));
  addTextLines(group, node.name_lines || [node.name], node.x + node.width / 2, node.y + 18, 'label-name');
  if (node.years) {
    const years = el('text', { x: node.x + node.width / 2, y: node.y + node.height - 8, class: 'label-year', 'text-anchor': 'middle' });
    years.textContent = node.years;
    group.appendChild(years);
  }
  const hit = el('rect', {
    x: node.x,
    y: node.y,
    width: node.width,
    height: node.height,
    class: 'child-hit'
  });
  hit.addEventListener('click', () => showPerson(node.person_id));
  group.appendChild(hit);
  return group;
}

function renderTree() {
  viewport.innerHTML = '';
  svg.setAttribute('viewBox', `0 0 ${Math.max(data.canvas.width, 100)} ${Math.max(data.canvas.height, 100)}`);
  data.edges.forEach(edge => viewport.appendChild(drawEdge(edge)));
  data.familyNodes.forEach(node => viewport.appendChild(drawFamily(node)));
  data.childNodes.forEach(node => viewport.appendChild(drawChild(node)));
  updateViewport();
}

function focusPerson(personId) {
  const targets = focusNodes.get(personId) || [];
  if (!targets.length) return;
  showPerson(personId);
  const family = data.familyNodes.find(node => node.family_id === targets[0]);
  const child = data.childNodes.find(node => node.node_id === targets[0]);
  const target = family || child;
  if (target) centerOnPoint(target.x + target.width / 2, target.y + target.height / 2);
}

function applySearch(rawValue) {
  const value = rawValue.trim().toLowerCase();
  if (!value) {
    selectedPersonId = null;
    detail.innerHTML = '<div class="empty-state">Select a person to view details.</div>';
    renderTree();
    return;
  }
  const match = data.persons.find(person => person.display_name.toLowerCase().includes(value));
  if (match) {
    focusPerson(match.person_id);
  }
}

document.getElementById('search').addEventListener('input', event => applySearch(event.target.value));
document.getElementById('zoom-in').addEventListener('click', () => { scale = Math.min(scale * 1.2, 2.6); updateViewport(); });
document.getElementById('zoom-out').addEventListener('click', () => { scale = Math.max(scale / 1.2, 0.2); updateViewport(); });
document.getElementById('reset-view').addEventListener('click', fitTree);
svg.addEventListener('mousedown', event => {
  dragState = { x: event.clientX, y: event.clientY, tx: translateX, ty: translateY };
  svg.classList.add('dragging');
});
window.addEventListener('mousemove', event => {
  if (!dragState) return;
  translateX = dragState.tx + (event.clientX - dragState.x);
  translateY = dragState.ty + (event.clientY - dragState.y);
  updateViewport();
});
window.addEventListener('mouseup', () => {
  dragState = null;
  svg.classList.remove('dragging');
});
svg.addEventListener('wheel', event => {
  event.preventDefault();
  const rect = svg.getBoundingClientRect();
  const mouseX = event.clientX - rect.left;
  const mouseY = event.clientY - rect.top;
  const worldX = (mouseX - translateX) / scale;
  const worldY = (mouseY - translateY) / scale;
  const nextScale = event.deltaY < 0 ? Math.min(scale * 1.1, 2.8) : Math.max(scale / 1.1, 0.18);
  translateX = mouseX - worldX * nextScale;
  translateY = mouseY - worldY * nextScale;
  scale = nextScale;
  updateViewport();
}, { passive: false });
window.addEventListener('resize', fitTree);
renderTree();
fitTree();
</script>
</body>
</html>
"""
