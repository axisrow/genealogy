import sqlite3
from pathlib import Path


SCHEMA = """
pragma foreign_keys = on;

create table if not exists persons (
    person_id text primary key,
    source_visio_id text,
    display_name text not null,
    given_name text,
    surname text,
    original_text text,
    birth text,
    christening text,
    death text,
    notes text
);

create table if not exists families (
    family_id text primary key,
    spouse1_person_id text references persons(person_id),
    spouse2_person_id text references persons(person_id),
    source_visio_id text,
    original_combined_text text,
    confidence text not null default 'high',
    review_status text not null default 'ok'
);

create table if not exists family_children (
    family_id text not null references families(family_id),
    child_person_id text not null references persons(person_id),
    source_visio_id text,
    primary key (family_id, child_person_id)
);

create table if not exists source_nodes (
    source_visio_id text primary key,
    primary_person_id text not null references persons(person_id),
    family_id text references families(family_id),
    original_text text
);

create table if not exists media (
    media_id integer primary key autoincrement,
    person_id text not null references persons(person_id),
    relative_file_path text not null,
    title text,
    media_type text,
    notes text
);

create table if not exists import_issues (
    issue_id integer primary key autoincrement,
    source_visio_id text,
    person_id text,
    issue_type text not null,
    message text not null,
    raw_text text
);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    db_path = Path(path)
    if db_path.parent and str(db_path.parent) != ".":
        db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma foreign_keys = on")
    return conn


def init_db(conn: sqlite3.Connection, reset: bool = False) -> None:
    if reset:
        conn.executescript(
            """
            drop table if exists import_issues;
            drop table if exists media;
            drop table if exists family_children;
            drop table if exists source_nodes;
            drop table if exists families;
            drop table if exists persons;
            """
        )
    conn.executescript(SCHEMA)
    conn.commit()
