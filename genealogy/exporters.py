from __future__ import annotations

import json
import sqlite3
from collections import defaultdict, deque
from pathlib import Path
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

    payload = {"persons": persons, "families": families, "children": children, "media": media}
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


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Genealogy Tree</title>
<style>
:root { color-scheme: light; font-family: Arial, sans-serif; }
body { margin: 0; background: #f6f7f9; color: #1c2430; }
header { padding: 16px 20px; background: #ffffff; border-bottom: 1px solid #d8dee8; display: flex; gap: 12px; align-items: center; }
h1 { font-size: 20px; margin: 0; }
#search { max-width: 360px; width: 40vw; padding: 9px 11px; border: 1px solid #aeb8c7; border-radius: 6px; font-size: 14px; }
main { display: grid; grid-template-columns: minmax(260px, 1fr) 340px; min-height: calc(100vh - 58px); }
#tree { padding: 18px 20px 40px; overflow: auto; }
#detail { background: #ffffff; border-left: 1px solid #d8dee8; padding: 18px; }
.node { margin: 4px 0 4px var(--depth); }
.row { display: inline-flex; align-items: center; gap: 8px; min-height: 30px; }
button.toggle { width: 24px; height: 24px; border: 1px solid #9aa6b6; background: #fff; border-radius: 4px; cursor: pointer; }
button.person { border: 1px solid #b9c2d0; background: #fff; border-radius: 6px; padding: 6px 10px; cursor: pointer; }
button.person:hover, button.person.active { border-color: #245b9d; background: #eaf2fb; }
.muted { color: #637083; font-size: 13px; }
.photo { max-width: 100%; border-radius: 6px; border: 1px solid #d8dee8; margin: 10px 0; }
@media (max-width: 760px) {
  header { flex-wrap: wrap; }
  #search { width: 100%; max-width: none; }
  main { grid-template-columns: 1fr; }
  #detail { border-left: 0; border-top: 1px solid #d8dee8; }
}
</style>
</head>
<body>
<header><h1>Genealogy Tree</h1><input id="search" type="search" placeholder="Search people"></header>
<main><section id="tree"></section><aside id="detail"><p class="muted">Select a person to view details.</p></aside></main>
<script id="tree-data" type="application/json">__DATA__</script>
<script>
const data = JSON.parse(document.getElementById('tree-data').textContent);
const people = new Map(data.persons.map(p => [p.person_id, p]));
const mediaByPerson = new Map();
for (const item of data.media) {
  if (!mediaByPerson.has(item.person_id)) mediaByPerson.set(item.person_id, []);
  mediaByPerson.get(item.person_id).push(item);
}
const childrenByFamily = new Map();
for (const link of data.children) {
  if (!childrenByFamily.has(link.family_id)) childrenByFamily.set(link.family_id, []);
  childrenByFamily.get(link.family_id).push(link.child_person_id);
}
const familiesBySpouse = new Map();
for (const family of data.families) {
  for (const pid of [family.spouse1_person_id, family.spouse2_person_id]) {
    if (!pid) continue;
    if (!familiesBySpouse.has(pid)) familiesBySpouse.set(pid, []);
    familiesBySpouse.get(pid).push(family);
  }
}
const childIds = new Set(data.children.map(c => c.child_person_id));
const roots = data.persons.filter(p => !childIds.has(p.person_id));
const collapsed = new Set();
const tree = document.getElementById('tree');
const detail = document.getElementById('detail');
function render(filter = '') {
  tree.innerHTML = '';
  const lower = filter.trim().toLowerCase();
  const shown = lower ? data.persons.filter(p => p.display_name.toLowerCase().includes(lower)) : roots;
  for (const person of shown) renderPerson(person.person_id, 0, new Set());
}
function renderPerson(personId, depth, seen) {
  if (seen.has(personId) || !people.has(personId)) return;
  seen.add(personId);
  const person = people.get(personId);
  const row = document.createElement('div');
  row.className = 'node';
  row.style.setProperty('--depth', `${depth * 22}px`);
  const children = [];
  for (const family of familiesBySpouse.get(personId) || []) children.push(...(childrenByFamily.get(family.family_id) || []));
  row.innerHTML = `<span class="row"><button class="toggle" title="Expand or collapse">${collapsed.has(personId) ? '+' : '-'}</button><button class="person">${person.display_name}</button><span class="muted">${person.birth || person.christening || ''}</span></span>`;
  row.querySelector('.toggle').onclick = () => { collapsed.has(personId) ? collapsed.delete(personId) : collapsed.add(personId); render(document.getElementById('search').value); };
  row.querySelector('.person').onclick = () => showPerson(personId);
  tree.appendChild(row);
  if (!collapsed.has(personId)) for (const childId of children) renderPerson(childId, depth + 1, new Set(seen));
}
function showPerson(personId) {
  const person = people.get(personId);
  const photos = mediaByPerson.get(personId) || [];
  detail.innerHTML = `<h2>${person.display_name}</h2>
    <p class="muted">${person.person_id}${person.source_visio_id ? ' | ' + person.source_visio_id : ''}</p>
    ${person.birth ? `<p><strong>Birth:</strong> ${person.birth}</p>` : ''}
    ${person.christening ? `<p><strong>Christening:</strong> ${person.christening}</p>` : ''}
    ${person.death ? `<p><strong>Death:</strong> ${person.death}</p>` : ''}
    ${person.notes ? `<p><strong>Notes:</strong> ${person.notes}</p>` : ''}
    ${photos.map(photo => `<figure><img class="photo" src="${photo.relative_file_path}" alt="${photo.title || person.display_name}"><figcaption>${photo.title || ''}</figcaption></figure>`).join('')}`;
}
document.getElementById('search').addEventListener('input', event => render(event.target.value));
render();
</script>
</body>
</html>
"""
