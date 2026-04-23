from __future__ import annotations

import html
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class VdxPosition:
    source_id: str
    name: str
    title: str = ""


@dataclass(frozen=True)
class VdxReport:
    source_id: str
    target_id: str


@dataclass(frozen=True)
class VdxData:
    positions: list[VdxPosition]
    reports: list[VdxReport]


POSITION_RE = re.compile(
    r"<mstns:Position\b(?P<attrs>[^>]*)>(?P<body>.*?)</mstns:Position>",
    re.IGNORECASE | re.DOTALL,
)
REPORT_RE = re.compile(r"<mstns:ReportsTo\b(?P<attrs>[^>]*)/?>", re.IGNORECASE | re.DOTALL)
ATTR_RE = re.compile(r"([A-Za-z_:][\w:.-]*)\s*=\s*(['\"])(.*?)\2", re.DOTALL)


def parse_attrs(text: str) -> dict[str, str]:
    return {name: html.unescape(value.strip()) for name, _, value in ATTR_RE.findall(text)}


def extract_tag(body: str, tag: str) -> str:
    match = re.search(
        rf"<mstns:{re.escape(tag)}\b[^>]*>(.*?)</mstns:{re.escape(tag)}>",
        body,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ""
    value = re.sub(r"<[^>]+>", "", match.group(1))
    return " ".join(html.unescape(value).split())


def parse_vdx_text(text: str) -> VdxData:
    positions: list[VdxPosition] = []
    for match in POSITION_RE.finditer(text):
        attrs = parse_attrs(match.group("attrs"))
        source_id = attrs.get("ID", "").strip()
        if not source_id:
            continue
        body = match.group("body")
        name = extract_tag(body, "Name")
        title = extract_tag(body, "Title")
        if name or title:
            positions.append(VdxPosition(source_id=source_id, name=name or title, title=title))

    reports: list[VdxReport] = []
    for match in REPORT_RE.finditer(text):
        attrs = parse_attrs(match.group("attrs"))
        source_id = attrs.get("SourceObject", "").strip()
        target_id = attrs.get("TargetObject", "").strip()
        if source_id and target_id:
            reports.append(VdxReport(source_id=source_id, target_id=target_id))

    return VdxData(positions=positions, reports=reports)


def parse_vdx_file(path: str | Path) -> VdxData:
    data = Path(path).read_text(encoding="utf-8", errors="replace")
    return parse_vdx_text(data)
