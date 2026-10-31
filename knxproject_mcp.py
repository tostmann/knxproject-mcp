#!/usr/bin/env python3
"""knxproject-mcp -- MCP server that keeps the record of a KNX installation.

An agent without a file system (claude.ai, for one) forgets an installation
between two conversations.  This server is its memory: topology, devices,
group addresses, com objects, links and parameters, each statement with the
source it came from, plus notes, findings and the rules of the house.  Python
standard library only.

    knxproject_mcp.py serve  [--data DIR] [--bind ADDR|auto] [--port N] ...
    knxproject_mcp.py init   --installation ID --name NAME [--data DIR]
    knxproject_mcp.py import FORMAT PATH... --installation ID [--as-of DATE] ...
    knxproject_mcp.py health [--data DIR]   (exit 0 if the running server is healthy)

Import formats: ets3 (a gzip CSV dump of an ETS3 project database), esf (ETS
group address export), inventory (the reply of a busware KNX stick's
knx_inventory), observe (knx_observe_list), ha-yaml (the knx: block of a Home
Assistant configuration), fhem (fhem.cfg, EIB/KNX devices) and claims (JSON
lines).  Bulk data never passes through the agent: imports are commands.

Every statement is a claim: (kind, key, attribute, value, source, when).
Several sources may say different things about the same group address; the
installation's precedence decides which value counts, the others stay
queryable.  Claims are never deleted: an import that no longer states one ends
it, the user withdraws one.  The source "user" is what the operator decided
(entered by the agent, with the operator's words); "bus" is what a device
holds.  Where the two disagree, the installation still has to be changed.

The writing tools change this record only.  They never talk to a KNX bus.

Every option has an environment fallback: KNXPROJECT_DATA, KNXPROJECT_BIND,
KNXPROJECT_PORT, KNXPROJECT_ALLOWED_HOSTS, KNXPROJECT_ALLOW_ANY_BIND,
KNXPROJECT_MDNS, KNXPROJECT_MDNS_NAME, KNXPROJECT_MDNS_HOSTNAME,
KNXPROJECT_MDNS_ADDRESS.  The server announces itself via mDNS/DNS-SD as
_knxproject-mcp._tcp (TXT path=/mcp).
"""

import argparse
import array
import collections
import contextlib
import csv
import datetime
import fcntl
import glob
import gzip
import ipaddress
import json
import os
import random
import re
import signal
import socket
import socketserver
import sqlite3
import struct
import sys
import threading
import time
import traceback
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

VERSION = "0.3.0"
SCHEMA_VERSION = 1
BRIEF_OPEN_MAX = 20  # open findings and todos the brief lists
BRIEF_ITEM_CHARS = 200  # an open item's text in the brief when it would not fit otherwise
RULE_CHARS = 400        # a rule's text in the brief when the rules would not fit otherwise
NOTE_CHARS = 300        # text and said of a note shown with an entry (proj-note list id=N: whole)
JOURNAL_CHARS = 4000    # a journal entry's args and result, each, as JSON
DB_NAME = "project.sqlite"
SERVER_NAME = "knxproject-mcp"

DEFAULT_DATA = "/data"
DEFAULT_BIND = "127.0.0.1"
DEFAULT_PORT = 8096

SUPPORTED_PROTOCOLS = ("2025-06-18", "2025-03-26", "2024-11-05")
DEFAULT_PROTOCOL = "2025-06-18"

# One tools/call reply, whole.  A busware stick forwards the calls of this
# server and holds every reply in RAM before passing it on.
RESPONSE_BUDGET = 12000
HARD_REPLY_CAP = 15500             # the stick drops a reply above 16 KB
MAX_MCP_BODY = 1 << 20

DEFAULT_PRECEDENCE = ["user", "bus", "observed", "ha", "fhem", "ets", "knxproj", "esf",
                      "product"]
SOURCES = frozenset(DEFAULT_PRECEDENCE)
KINDS = ("installation", "area", "line", "device", "range", "group", "object", "link",
         "param")
NOTE_KINDS = ("note", "finding", "rule", "todo")
NOTE_STATUS = ("open", "confirmed", "rejected", "done")

SKILL_URI = "skill://knxproject-mcp/SKILL.md"
SKILL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "skill", "SKILL.md")
SKILL_DESCRIPTION = ("Read this first: how the record is built (claims, sources, "
                     "precedence), the workflow, what to write and what never to assume.")

INSTRUCTIONS = (
    "knxproject-mcp is the record of a KNX installation: topology, devices, group "
    "addresses, com objects, links, parameters, and the notes, findings and rules of "
    "the house. Every value carries its source. START every conversation with "
    "proj-brief: it holds the description and the binding rules of the installation. "
    "How to work with it: resource " + SKILL_URI + ". The writing tools (proj-set, "
    "proj-note) change this record only, never the bus. proj-read-device reads one device "
    "through the installation's KNX stick (bus traffic, reads only): only when the operator "
    "asked for it."
)


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------

_WS_RE = re.compile(r"\s+")


def _dumps(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _clean(s, maxlen=None):
    s = _WS_RE.sub(" ", str(s or "")).strip()
    if maxlen is not None and len(s) > maxlen:
        s = s[:maxlen - 1] + "…"
    return s


def _utc_now():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_ts():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


_TS_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?)?")


def norm_ts(value):
    """'2015-05-29 12:25:14', '2015-05-29T12:25', '2015-05-29' -> sortable
    'YYYY-MM-DDTHH:MM:SS'; None if it is none of these."""
    m = _TS_RE.match(str(value or "").strip())
    if not m:
        return None
    y, mo, d, h, mi, s = m.groups()
    try:
        datetime.datetime(int(y), int(mo), int(d), int(h or 0), int(mi or 0), int(s or 0))
    except ValueError:
        return None
    return "%s-%s-%sT%s:%s:%s" % (y, mo, d, h or "00", mi or "00", s or "00")


def _fsync_dir(path):
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_json(path, obj):
    tmp = "%s.%d.tmp" % (path, os.getpid())
    with open(tmp, "wb") as f:
        f.write(_dumps(obj).encode("utf-8"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_dir(os.path.dirname(path) or ".")


def _load_json(path, default=None):
    try:
        with open(path, "rb") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


# ---------------------------------------------------------------------------
# KNX addresses and the keys of the record
# ---------------------------------------------------------------------------

_IA_RE = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{1,3})$")
_LINE_RE = re.compile(r"^(\d{1,2})\.(\d{1,2})$")
_GA3_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{1,3})$")
_GA2_RE = re.compile(r"^(\d{1,2})/(\d{1,4})$")
_NUM_RE = re.compile(r"^\d{1,5}$")


def norm_ia(value):
    """'1.2.3' -> '1.2.3'; None if not an individual address."""
    m = _IA_RE.match(str(value or "").strip())
    if not m:
        return None
    a, l, d = (int(x) for x in m.groups())
    if a > 15 or l > 15 or d > 255:
        return None
    return "%d.%d.%d" % (a, l, d)


def norm_line(value):
    m = _LINE_RE.match(str(value or "").strip())
    if not m:
        return None
    a, l = (int(x) for x in m.groups())
    if a > 15 or l > 15:
        return None
    return "%d.%d" % (a, l)


def norm_ga(value):
    """Three-level group address text; also takes a 16-bit number."""
    if isinstance(value, int) and not isinstance(value, bool):
        return ga_from_int(value) if 0 <= value <= 0xFFFF else None
    s = str(value or "").strip()
    m = _GA3_RE.match(s)
    if not m:
        return None
    a, b, c = (int(x) for x in m.groups())
    if a > 31 or b > 7 or c > 255:
        return None
    return "%d/%d/%d" % (a, b, c)


def ga_int(ga):
    a, b, c = (int(x) for x in ga.split("/"))
    return (a << 11) | (b << 8) | c


def ga_from_int(n):
    return "%d/%d/%d" % ((n >> 11) & 0x1F, (n >> 8) & 0x07, n & 0xFF)


def fhem_hex_ga(h):
    """FHEM's EIB module writes an unnamed group address as 4 hex digits,
    split 4/4/8 bits (3107 = 3/1/7), not the 5/3/8 of the KNX standard."""
    if not re.match(r"^[0-9a-fA-F]{4}$", h or ""):
        return None
    n = int(h, 16)
    return norm_ga("%d/%d/%d" % (n >> 12, (n >> 8) & 0xF, n & 0xFF))


def ia_sort(ia):
    return tuple(int(x) for x in ia.split("."))


def obj_key(ia, number):
    return "%s#%d" % (ia, int(number))


def link_key(ia, number, ga):
    return "%s#%d>%s" % (ia, int(number), ga)


def split_link(key):
    """'1.2.3#24>3/1/7' -> ('1.2.3', 24, '3/1/7')."""
    left, ga = key.split(">", 1)
    ia, num = left.split("#", 1)
    return ia, int(num), ga


def key_sort(kind, key):
    try:
        if kind == "device":
            return (0, ia_sort(key))
        if kind == "group":
            return (0, (ga_int(key),))
        if kind in ("object", "link"):
            ia, rest = key.split("#", 1)
            num = rest.split(">", 1)[0]
            tail = ga_int(rest.split(">", 1)[1]) if ">" in rest else -1
            return (0, ia_sort(ia) + (int(num), tail))
        if kind == "range":
            return (0, tuple(int(x) for x in key.split("/")))
        if kind in ("area", "line"):
            return (0, tuple(int(x) for x in key.split(".")))
        if kind == "param":
            ia, ref = key.split(":", 1)
            return (0, ia_sort(ia) + (_natural(ref),))
    except (ValueError, IndexError):
        pass
    return (1, (key,))


def _natural(s):
    m = re.search(r"\d+", s)
    return int(m.group(0)) if m else 0


class RefError(ValueError):
    pass


def parse_ref(text):
    """A reference to one entry of the record -> (kind, key).

    1.2.3 device, 1.2 line, 3/1/7 group address, 3/1 middle group,
    1.2.3#24 com object, 1.2.3#24>3/1/7 link, 1.2.3:P61 parameter,
    and explicit forms kind:key (area:1, range:3, line:1.2, installation)."""
    s = str(text or "").strip()
    if not s:
        raise RefError("empty reference")
    if s.lower() in ("installation", "installation:-", "-"):
        return "installation", "-"
    m = re.match(r"^(area|line|device|range|group|object|link|param):(.+)$", s, re.I)
    if m:
        kind, rest = m.group(1).lower(), m.group(2).strip()
        if kind == "area":
            if _NUM_RE.match(rest) and int(rest) <= 15:
                return kind, str(int(rest))
            raise RefError("not an area: %r" % rest)
        if kind == "range":
            if _NUM_RE.match(rest) and int(rest) <= 31:
                return kind, str(int(rest))
            m2 = _GA2_RE.match(rest)
            if m2 and int(m2.group(1)) <= 31 and int(m2.group(2)) <= 7:
                return kind, "%d/%d" % (int(m2.group(1)), int(m2.group(2)))
            raise RefError("not a group range: %r (3 or 3/1)" % rest)
        return parse_ref(rest) if kind != "installation" else ("installation", "-")
    if ">" in s:
        left, ga = s.split(">", 1)
        k, key = parse_ref(left)
        g = norm_ga(ga)
        if k != "object" or g is None:
            raise RefError("a link is device#object>group address: %r" % s)
        ia, num = key.split("#")
        return "link", link_key(ia, num, g)
    if "#" in s:
        ia, num = s.split("#", 1)
        ia = norm_ia(ia)
        if ia is None or not _NUM_RE.match(num.strip()):
            raise RefError("a com object is device#number: %r" % s)
        return "object", obj_key(ia, num.strip())
    if ":" in s:
        ia, ref = s.split(":", 1)
        ia = norm_ia(ia)
        ref = ref.strip()
        if ia is None or not ref or len(ref) > 80:
            raise RefError("a parameter is device:ref: %r" % s)
        return "param", "%s:%s" % (ia, ref)
    ia = norm_ia(s)
    if ia:
        return "device", ia
    line = norm_line(s)
    if line:
        return "line", line
    ga = norm_ga(s)
    if ga:
        return "group", ga
    m = _GA2_RE.match(s)
    if m and int(m.group(1)) <= 31 and int(m.group(2)) <= 7:
        return "range", "%d/%d" % (int(m.group(1)), int(m.group(2)))
    raise RefError("not a reference: %r (examples: 1.2.3, 3/1/7, 3/1, 1.2, "
                   "1.2.3#24, area:1, range:3, installation)" % s)


def ref_text(kind, key):
    if kind in ("device", "group", "object", "link", "param", "line"):
        return key
    if kind == "range" and "/" in key:
        return key
    if kind == "installation":
        return "installation"
    return "%s:%s" % (kind, key)


# ---------------------------------------------------------------------------
# the store: one SQLite database
# ---------------------------------------------------------------------------

# What proj-find searches.  The partial index below is built from this list, and
# the find statement names it as literals: with bound parameters SQLite cannot
# prove that a query lies within a partial index's WHERE and does not use it.
_FIND_ATTRS = ("name", "description", "comment", "function", "product", "order_number",
               "location", "app_name", "text", "path", "role", "fhem_autocreate", "app_id",
               "serial", "order_info", "slot_name", "dpt", "eis")
assert all(re.match(r"^[a-z_]+$", a) for a in _FIND_ATTRS)
_FIND_ATTRS_SQL = ", ".join("'%s'" % a for a in _FIND_ATTRS)
_CLAIM_COLS = "kind, key, attr, value, source, ref, first_seen, last_seen, ended_at, id"

_DB_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)",
    "CREATE TABLE IF NOT EXISTS installations(id TEXT PRIMARY KEY, name TEXT NOT NULL, "
    " created TEXT NOT NULL, settings TEXT NOT NULL DEFAULT '{}')",
    "CREATE TABLE IF NOT EXISTS imports(id INTEGER PRIMARY KEY, inst TEXT NOT NULL, "
    " source TEXT NOT NULL, format TEXT NOT NULL, ref TEXT, seen TEXT, at TEXT NOT NULL, "
    " by TEXT, stats TEXT)",
    "CREATE TABLE IF NOT EXISTS claims(id INTEGER PRIMARY KEY, inst TEXT NOT NULL, "
    " kind TEXT NOT NULL, key TEXT NOT NULL, attr TEXT NOT NULL, value TEXT NOT NULL, "
    " source TEXT NOT NULL, ref TEXT, first_seen TEXT, last_seen TEXT, ended_at TEXT, "
    " import_id INTEGER, recorded_at TEXT NOT NULL, by TEXT, "
    " UNIQUE(inst, kind, key, attr, source, value))",
    "CREATE INDEX IF NOT EXISTS claims_entity ON claims(inst, kind, key)",
    "CREATE INDEX IF NOT EXISTS claims_source ON claims(inst, source, ended_at)",
    # proj-brief counts current entries per kind from this index alone; without it
    # every index entry costs a row lookup (measured on an ESP32-S3: 17 s -> 2.5 s
    # from an SD card, 0.44 s from internal flash)
    "CREATE INDEX IF NOT EXISTS claims_kind_open ON claims(inst, kind, ended_at, key)",
    # everything proj-find reads, and only that.  The version in the name stands for
    # the definition: a changed one needs a new name (IF NOT EXISTS would keep the
    # old index, unused) and the old name dropped.
    "DROP INDEX IF EXISTS claims_find_v1",
    "DROP INDEX IF EXISTS claims_find_v2",
    "CREATE INDEX IF NOT EXISTS claims_find_v3 ON claims(inst, kind, key, attr, value, "
    "source, ref, first_seen, last_seen, ended_at) WHERE attr IN (" + _FIND_ATTRS_SQL + ") "
    "AND ended_at IS NULL",
    "CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, inst TEXT NOT NULL, "
    " kind TEXT NOT NULL, status TEXT NOT NULL, text TEXT NOT NULL, said TEXT, by TEXT, "
    " created TEXT NOT NULL, updated TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS note_refs(note_id INTEGER NOT NULL, kind TEXT NOT NULL, "
    " key TEXT NOT NULL, PRIMARY KEY(note_id, kind, key))",
    "CREATE INDEX IF NOT EXISTS note_refs_entity ON note_refs(kind, key)",
    "CREATE TABLE IF NOT EXISTS journal(id INTEGER PRIMARY KEY, inst TEXT, at TEXT NOT NULL, "
    " by TEXT, tool TEXT NOT NULL, args TEXT, result TEXT)",
)


class StoreError(Exception):
    pass


class Store:
    """Short-lived connections, one per tool call or import; SQLite in WAL mode
    lets readers run while an import writes."""

    def __init__(self, data_dir):
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.path = os.path.join(self.data_dir, DB_NAME)
        with self.writer() as con:
            for stmt in _DB_SCHEMA:
                con.execute(stmt)
            row = con.execute("SELECT v FROM meta WHERE k='schema'").fetchone()
            if row is None:
                con.execute("INSERT INTO meta(k, v) VALUES('schema', ?)", (str(SCHEMA_VERSION),))
            elif row[0] != str(SCHEMA_VERSION):
                raise StoreError("database %s has schema %s, this program writes %s"
                                 % (self.path, row[0], SCHEMA_VERSION))

    def _connect(self):
        con = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA busy_timeout=30000")
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=FULL")
        return con

    @contextlib.contextmanager
    def reader(self):
        con = self._connect()
        try:
            con.execute("BEGIN")
            yield con
            con.execute("COMMIT")
        finally:
            con.close()

    @contextlib.contextmanager
    def writer(self):
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            try:
                yield con
            except BaseException:
                con.execute("ROLLBACK")
                raise
            con.execute("COMMIT")
        finally:
            con.close()

    def stats(self):
        with self.reader() as con:
            n_inst = con.execute("SELECT count(*) FROM installations").fetchone()[0]
            n_claims = con.execute("SELECT count(*) FROM claims WHERE ended_at IS NULL"
                                   ).fetchone()[0]
        return {"installations": n_inst, "claims": n_claims}


def create_installation(store, inst_id, name, precedence=None):
    if not re.match(r"^[a-z0-9][a-z0-9_-]{0,39}$", inst_id or ""):
        raise StoreError("installation id: lower-case letters, digits, - and _, "
                         "at most 40: %r" % inst_id)
    settings = {"precedence": list(precedence or DEFAULT_PRECEDENCE)}
    with store.writer() as con:
        if con.execute("SELECT 1 FROM installations WHERE id=?", (inst_id,)).fetchone():
            raise StoreError("installation %r exists" % inst_id)
        con.execute("INSERT INTO installations(id, name, created, settings) VALUES(?,?,?,?)",
                    (inst_id, _clean(name, 120) or inst_id, _now_ts(), _dumps(settings)))


def installation_row(con, inst_id):
    row = con.execute("SELECT * FROM installations WHERE id=?", (inst_id,)).fetchone()
    if row is None:
        raise StoreError("no installation %r" % inst_id)
    return row


def precedence_of(con, inst):
    """The installation's precedence, overridden by a user claim if one exists."""
    row = con.execute("SELECT value FROM claims WHERE inst=? AND kind='installation' AND "
                      "key='-' AND attr='precedence' AND source='user' AND ended_at IS NULL "
                      "ORDER BY id DESC LIMIT 1", (inst,)).fetchone()
    if row is not None:
        val = json.loads(row[0])
        if isinstance(val, list) and val:
            return _complete_precedence(val)
    settings = json.loads(installation_row(con, inst)["settings"] or "{}")
    return _complete_precedence(settings.get("precedence") or DEFAULT_PRECEDENCE)


def _complete_precedence(order):
    out = [s for s in order if s in SOURCES]
    return out + [s for s in DEFAULT_PRECEDENCE if s not in out]


# ---------------------------------------------------------------------------
# claims: writing
# ---------------------------------------------------------------------------

def put_claims(con, inst, source, claims, seen, import_id=None, by=None, scope=None):
    """Assert claims [(kind, key, attr, value, ref)] for `source`, seen at
    `seen`.  `scope(kind, key, attr) -> bool` names what this assertion speaks
    for completely: current claims of the source inside it that were not
    asserted again end at `seen`.  An assertion older than the newest current
    claim the source holds inside the scope extends history and ends nothing,
    and what it states for the first time arrives already ended: the newer
    assertion of the same scope did not state it.  The comparison stays inside
    the scope, so one device's read does not depend on another device's date."""
    if source not in SOURCES:
        raise StoreError("unknown source %r" % source)
    seen = norm_ts(seen) or _now_ts()
    now = _now_ts()
    asserted = set()
    st = {"asserted": 0, "new": 0, "reopened": 0, "ended": 0}
    covered, newest = [], None
    if scope is not None:
        covered = [r for r in con.execute(
            "SELECT id, kind, key, attr, last_seen FROM claims WHERE inst=? AND source=? AND "
            "ended_at IS NULL", (inst, source)).fetchall()
            if scope(r["kind"], r["key"], r["attr"])]
        newest = max((r["last_seen"] for r in covered if r["last_seen"]), default=None)
    stale = newest is not None and seen < newest
    if stale:
        st["record_newer"] = newest
        st["arrived_ended"] = 0
    for kind, key, attr, value, ref in claims:
        if kind not in KINDS:
            raise StoreError("unknown kind %r" % kind)
        vj = _dumps(value)
        row = con.execute("SELECT id, first_seen, last_seen, ended_at FROM claims WHERE "
                          "inst=? AND kind=? AND key=? AND attr=? AND source=? AND value=?",
                          (inst, kind, key, attr, source, vj)).fetchone()
        if row is None:
            ended = newest if stale and scope(kind, key, attr) else None
            cur = con.execute(
                "INSERT INTO claims(inst, kind, key, attr, value, source, ref, first_seen, "
                "last_seen, ended_at, import_id, recorded_at, by) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (inst, kind, key, attr, vj, source, ref, seen, seen, ended, import_id, now, by))
            asserted.add(cur.lastrowid)
            st["arrived_ended" if ended else "new"] += 1
        else:
            first = min(row["first_seen"] or seen, seen)
            last = max(row["last_seen"] or seen, seen)
            ended = row["ended_at"]
            if ended is not None and seen >= ended:
                ended = None
                st["reopened"] += 1
            con.execute("UPDATE claims SET first_seen=?, last_seen=?, ended_at=?, "
                        "import_id=COALESCE(?, import_id), ref=COALESCE(?, ref) WHERE id=?",
                        (first, last, ended, import_id, ref, row["id"]))
            asserted.add(row["id"])
        st["asserted"] += 1
    if scope is not None and not stale:
        for r in covered:
            if r["id"] not in asserted:
                con.execute("UPDATE claims SET ended_at=? WHERE id=?", (seen, r["id"]))
                st["ended"] += 1
    return st


def _shorten(obj, n=200):
    """obj with strings cut to n characters and lists to 20 items."""
    if isinstance(obj, str):
        return obj if len(obj) <= n else obj[:n] + "…"
    if isinstance(obj, list):
        out = [_shorten(x, n) for x in obj[:20]]
        if len(obj) > 20:
            out.append("… %d more" % (len(obj) - 20))
        return out
    if isinstance(obj, dict):
        return {k: _shorten(v, n) for k, v in obj.items()}
    return obj


def _journal_text(obj):
    """obj as JSON for the journal: about JOURNAL_CHARS at most, and JSON always
    (a cut JSON text could not be read back)."""
    s = _dumps(obj)
    if len(s) <= JOURNAL_CHARS:
        return s
    s = _dumps(_shorten(obj, 300))
    if len(s) <= JOURNAL_CHARS:
        return s
    return _dumps({"cut": s[:JOURNAL_CHARS - 100]})


def _journal_value(text):
    """A journal column read back; an entry an older version cut stays text."""
    if not text:
        return None
    try:
        return json.loads(text)
    except ValueError:
        return text


def journal(con, inst, by, tool, args, result):
    con.execute("INSERT INTO journal(inst, at, by, tool, args, result) VALUES(?,?,?,?,?,?)",
                (inst, _now_ts(), by, tool, _journal_text(args), _journal_text(result)))


# ---------------------------------------------------------------------------
# claims: reading
# ---------------------------------------------------------------------------

def load_claims(con, inst, kind=None, keys=None, attrs=None, ended=False, source=None):
    """Claim rows; keys may be a list (exact) or a 'prefix*' string."""
    q = ["SELECT kind, key, attr, value, source, ref, first_seen, last_seen, ended_at, id "
         "FROM claims WHERE inst=?"]
    p = [inst]
    if kind is not None:
        q.append("AND kind=?")
        p.append(kind)
    if isinstance(keys, str) and keys.endswith("*"):
        q.append("AND key >= ? AND key < ?")
        p += [keys[:-1], keys[:-1] + "\U0010ffff"]
    elif keys is not None:
        keys = list(keys)
        if not keys:
            return []
        if len(keys) > 500:
            rows = load_claims(con, inst, kind, None, attrs, ended, source)
            ks = set(keys)
            return [r for r in rows if r["key"] in ks]
        q.append("AND key IN (%s)" % ",".join("?" * len(keys)))
        p += keys
    if attrs is not None:
        attrs = list(attrs)
        q.append("AND attr IN (%s)" % ",".join("?" * len(attrs)))
        p += attrs
    if source is not None:
        q.append("AND source=?")
        p.append(source)
    if not ended:
        q.append("AND ended_at IS NULL")
    out = []
    for r in con.execute(" ".join(q), p):
        d = dict(r)
        d["value"] = json.loads(d["value"])
        out.append(d)
    return out


def by_entity(rows):
    out = collections.defaultdict(list)
    for r in rows:
        out[(r["kind"], r["key"])].append(r)
    return out


def resolve(rows, precedence):
    """{attr: {"value", "source", ...}} -- per attribute the claim of the first
    source in `precedence`, the newest within it; `also` lists other current
    values of the same source, `others` how many other sources disagree."""
    rank = {s: i for i, s in enumerate(precedence)}
    per = collections.defaultdict(list)
    for r in rows:
        if r["ended_at"] is None:
            per[r["attr"]].append(r)
    out = {}
    for attr, rs in per.items():
        rs.sort(key=lambda r: (rank.get(r["source"], 99), -(_ts_num(r["last_seen"])), -r["id"]))
        top = rs[0]
        e = {"value": top["value"], "source": top["source"]}
        also = [r["value"] for r in rs[1:] if r["source"] == top["source"]
                and r["value"] != top["value"]]
        if also:
            e["also"] = also
        differ = sorted({r["source"] for r in rs if r["value"] != top["value"]
                         and r["source"] != top["source"]})
        if differ:
            e["differs"] = differ
        out[attr] = e
    return out


def _ts_num(ts):
    if not ts:
        return 0
    return int(re.sub(r"\D", "", ts)[:14] or 0)


def value_of(res, attr, default=None):
    e = res.get(attr)
    return e["value"] if e else default


def sources_saying(rows, attr, value=True):
    return sorted({r["source"] for r in rows if r["attr"] == attr and r["value"] == value
                   and r["ended_at"] is None})


def claim_history(rows):
    """Compact list of every claim, newest first per attribute."""
    out = []
    for r in sorted(rows, key=lambda r: (r["attr"], r["source"], -_ts_num(r["last_seen"]))):
        e = {"attr": r["attr"], "value": r["value"], "source": r["source"]}
        if r["first_seen"]:
            e["seen"] = (r["first_seen"][:10] if r["first_seen"][:10] == (r["last_seen"] or "")[:10]
                         else "%s..%s" % (r["first_seen"][:10], (r["last_seen"] or "")[:10]))
        if r["ended_at"]:
            e["ended"] = r["ended_at"][:10]
        if r["ref"]:
            e["ref"] = r["ref"]
        out.append(e)
    return out


def device_label(res):
    for attr in ("name", "description", "product"):
        v = value_of(res, attr)
        if v:
            return v
    return ""


# ---------------------------------------------------------------------------
# imports
# ---------------------------------------------------------------------------

class ImportFailed(Exception):
    pass


def record_import(store, inst, source, fmt, ref, seen, claims, scope, by=None, date_from=None):
    """date_from: "data" (the input states it), "given" (--as-of) or "file" (only the
    file's modification time is known; the brief then calls the date unknown)."""
    seen = norm_ts(seen) or _now_ts()
    claims = list(collections.OrderedDict(
        ((k, key, a, _dumps(v)), (k, key, a, v, r)) for k, key, a, v, r in claims).values())
    with store.writer() as con:
        installation_row(con, inst)
        cur = con.execute("INSERT INTO imports(inst, source, format, ref, seen, at, by) "
                          "VALUES(?,?,?,?,?,?,?)",
                          (inst, source, fmt, ref, seen, _now_ts(), by or "cli"))
        iid = cur.lastrowid
        st = put_claims(con, inst, source, claims, seen, import_id=iid, by=by or "cli",
                        scope=scope)
        st["seen"] = seen
        if date_from:
            st["date_from"] = date_from
        con.execute("UPDATE imports SET stats=? WHERE id=?", (_dumps(st), iid))
        journal(con, inst, by or "cli", "import:" + fmt, {"ref": ref, "seen": seen}, st)
    return st


def _file_date(path):
    return datetime.datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%dT%H:%M:%S")


# -- ETS3 database dump ------------------------------------------------------

def _ets3_table(dump, table, want=None):
    path = os.path.join(dump, "data.delta.%s.csv.gz" % table)
    if not os.path.isfile(path):
        cands = sorted(glob.glob(os.path.join(dump, "data.*.%s.csv.gz" % table)))
        if not cands:
            raise ImportFailed("the ETS3 dump in %s has no table %s" % (dump, table))
        path = cands[0]
    csv.field_size_limit(1 << 30)
    with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        head = next(r)
        for row in r:
            d = dict(zip(head, row))
            if want is None or want(d):
                yield d


def _int(s, default=None):
    try:
        return int(str(s).strip())
    except (TypeError, ValueError):
        return default


def ets3_app_id(manufacturer, device_type, version, mask):
    """ApplicationId as the device reports it: 8 hex digits for BCU1 masks
    (one manufacturer byte), 10 otherwise."""
    if manufacturer is None or device_type is None or version is None:
        return None
    if mask is not None and 0x0010 <= mask <= 0x001F and manufacturer <= 0xFF:
        return "%02X%04X%02X" % (manufacturer, device_type & 0xFFFF, version & 0xFF)
    return "%04X%04X%02X" % (manufacturer & 0xFFFF, device_type & 0xFFFF, version & 0xFF)


def _fold(s):
    return _clean(s).casefold()


def import_ets3(dump, project=None):
    """-> (claims, seen, ref).  Reads the project tables of an ETS3 database
    dump (one gzip CSV per table, as written by an ODBC export)."""
    projects = list(_ets3_table(dump, "PROJECT"))
    if not projects:
        raise ImportFailed("no project in the dump")
    if project is None:
        if len(projects) > 1:
            raise ImportFailed("the dump holds %d projects, choose one with --project: %s"
                               % (len(projects), ", ".join(p["PROJECT_NAME"] for p in projects)))
        proj = projects[0]
    else:
        hit = [p for p in projects if _fold(p["PROJECT_NAME"]) == _fold(project)
               or p["PROJECT_ID"] == str(project)]
        if not hit:
            raise ImportFailed("no project %r in the dump: %s"
                               % (project, ", ".join(p["PROJECT_NAME"] for p in projects)))
        proj = hit[0]
    pid = proj["PROJECT_ID"]
    ref = "ets3:%s" % _clean(proj["PROJECT_NAME"])
    claims = [("installation", "-", "name", _clean(proj["PROJECT_NAME"]), ref)]

    media = {m["MEDIUM_TYPE_NUMBER"]: m["MEDIUM_TYPE_SHORT_NAME"]
             for m in _ets3_table(dump, "MEDIUM_TYPE")}
    areas = {a["AREA_ID"]: a for a in _ets3_table(dump, "AREA", lambda d: d["PROJECT_ID"] == pid)}
    lines = {l["LINE_ID"]: l for l in _ets3_table(dump, "LINE", lambda d: d["AREA_ID"] in areas)}
    for a in areas.values():
        key = str(_int(a["AREA_ADDRESS"], 0))
        if _clean(a["AREA_NAME"]):
            claims.append(("area", key, "name", _clean(a["AREA_NAME"]), ref))
        if _clean(a["AREA_DESCRIPTION"]):
            claims.append(("area", key, "description", _clean(a["AREA_DESCRIPTION"]), ref))
    line_key = {}
    for lid, l in lines.items():
        key = "%d.%d" % (_int(areas[l["AREA_ID"]]["AREA_ADDRESS"], 0), _int(l["LINE_ADDRESS"], 0))
        line_key[lid] = key
        if _clean(l["LINE_NAME"]):
            claims.append(("line", key, "name", _clean(l["LINE_NAME"]), ref))
        if _clean(l["LINE_DESCRIPTION"]):
            claims.append(("line", key, "description", _clean(l["LINE_DESCRIPTION"]), ref))
        if media.get(l["MEDIUM_TYPE_NUMBER"]):
            claims.append(("line", key, "medium", media[l["MEDIUM_TYPE_NUMBER"]], ref))

    devices = {d["DEVICE_ID"]: d for d in _ets3_table(dump, "DEVICE",
                                                      lambda d: d["PROJECT_ID"] == pid)}
    progs_needed = {d["PROGRAM_ID"] for d in devices.values()}
    cats_needed = {d["CATALOG_ENTRY_ID"] for d in devices.values()}
    progs = {p["PROGRAM_ID"]: p for p in _ets3_table(
        dump, "APPLICATION_PROGRAM", lambda d: d["PROGRAM_ID"] in progs_needed)}
    cats = {c["CATALOG_ENTRY_ID"]: c for c in _ets3_table(
        dump, "CATALOG_ENTRY", lambda d: d["CATALOG_ENTRY_ID"] in cats_needed)}
    mfrs = {m["MANUFACTURER_ID"]: _clean(m["MANUFACTURER_NAME"])
            for m in _ets3_table(dump, "MANUFACTURER")}
    masks = {m["MASK_ID"]: _int(m["MASK_VERSION"]) for m in _ets3_table(dump, "MASK")}
    programming = {p["DEVICE_ID"]: p for p in _ets3_table(
        dump, "DEVICE_PROGRAMMING", lambda d: d["DEVICE_ID"] in devices)}

    dev_ia = {}
    stamps = []
    for did, d in devices.items():
        lk = line_key.get(d["LINE_ID"])
        if lk is None:
            continue
        ia = norm_ia("%s.%d" % (lk, _int(d["DEVICE_ADDRESS"], 0)))
        if ia is None:
            continue
        dev_ia[did] = ia
        claims.append(("device", ia, "present", True, ref))
        for attr, col in (("name", "Name"), ("description", "DEVICE_DESCRIPTION"),
                          ("comment", "Comment")):
            if _clean(d.get(col)):
                claims.append(("device", ia, attr, _clean(d[col]), ref))
        cat = cats.get(d["CATALOG_ENTRY_ID"])
        if cat:
            if _clean(cat["ORDER_NUMBER"]):
                claims.append(("device", ia, "order_number", _clean(cat["ORDER_NUMBER"]), ref))
            if _clean(cat["ENTRY_NAME"]):
                claims.append(("device", ia, "product", _clean(cat["ENTRY_NAME"]), ref))
        prog = progs.get(d["PROGRAM_ID"])
        if prog:
            mfr = _int(prog["MANUFACTURER_ID"])
            mask = masks.get(prog["MASK_ID"])
            if mfrs.get(prog["MANUFACTURER_ID"]):
                claims.append(("device", ia, "manufacturer", mfrs[prog["MANUFACTURER_ID"]], ref))
            if _clean(prog["PROGRAM_NAME"]):
                claims.append(("device", ia, "app_name", _clean(prog["PROGRAM_NAME"]), ref))
            if mask is not None:
                claims.append(("device", ia, "mask", "0x%04X" % mask, ref))
            app = ets3_app_id(mfr, _int(prog["DEVICE_TYPE"]), _int(prog["PROGRAM_VERSION"]), mask)
            if app:
                claims.append(("device", ia, "app_id", app, ref))
        hi, lo = _clean(d.get("BAU_SERIAL_NUMBER_HIGH")), _clean(d.get("BAU_SERIAL_NUMBER_LOW"))
        if hi or lo:
            claims.append(("device", ia, "serial", "%s%s" % (hi, lo), ref))
        pr = programming.get(did)
        if pr:
            for attr, col in (("last_modified", "LAST_MODIFIED"), ("last_download", "LAST_DOWNLOAD")):
                ts = norm_ts(pr.get(col))
                if ts:
                    claims.append(("device", ia, attr, ts, ref))
                    stamps.append(ts)

    # group addresses
    mains = {m["MAINGROUP_ID"]: m for m in _ets3_table(dump, "MAINGROUP",
                                                       lambda d: d["PROJECT_ID"] == pid)}
    mids = {m["MIDDLEGROUP_ID"]: m for m in _ets3_table(
        dump, "MIDDLEGROUP", lambda d: d["MAINGROUP_ID"] in mains)}
    for m in mains.values():
        if _clean(m["MAINGROUP_NAME"]):
            claims.append(("range", str(_int(m["MAINGROUP_ADDRESS"], 0)), "name",
                           _clean(m["MAINGROUP_NAME"]), ref))
    mid_key = {}
    for mid, m in mids.items():
        k = "%d/%d" % (_int(mains[m["MAINGROUP_ID"]]["MAINGROUP_ADDRESS"], 0),
                       _int(m["MIDDLEGROUP_ADDRESS"], 0))
        mid_key[mid] = k
        if _clean(m["MIDDLEGROUP_NAME"]):
            claims.append(("range", k, "name", _clean(m["MIDDLEGROUP_NAME"]), ref))
    sub_ga = {}
    for s in _ets3_table(dump, "SUBGROUP", lambda d: d["MIDDLEGROUP_ID"] in mids):
        ga = norm_ga("%s/%d" % (mid_key[s["MIDDLEGROUP_ID"]], _int(s["SUBGROUP_ADDRESS"], 0)))
        if ga is None:
            continue
        sub_ga[s["SUBGROUP_ID"]] = ga
        claims.append(("group", ga, "present", True, ref))
        for attr, col in (("name", "SUBGROUP_NAME"), ("description", "SUBGROUP_DESCRIPTION"),
                          ("comment", "Comment")):
            if _clean(s.get(col)):
                claims.append(("group", ga, attr, _clean(s[col]), ref))
        for attr, col in (("central", "SUBGROUP_CENTRAL"), ("passthrough", "SUBGROUP_PASSTHROUGH")):
            if _clean(s.get(col)) in ("1", "-1"):
                claims.append(("group", ga, attr, True, ref))

    # com objects and links
    otypes = {o["OBJECT_TYPE_CODE"]: o["OBJECT_TYPE_NAME"] for o in _ets3_table(dump, "OBJECT_TYPE")}
    prios = {o["OBJECT_PRIORITY_CODE"]: o["OBJECT_PRIORITY_NAME"]
             for o in _ets3_table(dump, "OBJECT_PRIORITY")}
    dobjs = {o["DEVICE_OBJECT_ID"]: o for o in _ets3_table(
        dump, "DEVICE_OBJECT", lambda d: d["DEVICE_ID"] in dev_ia)}
    s2o = [x for x in _ets3_table(dump, "SUBGROUP_TO_OBJECT",
                                  lambda d: d["DEVICE_OBJECT_ID"] in dobjs)]
    linked = {x["DEVICE_OBJECT_ID"] for x in s2o}
    wanted = {k: o for k, o in dobjs.items() if o["DEVICE_OBJECT_VISIBLE"] == "1" or k in linked}
    obj_ids = {o["OBJECT_ID"] for o in wanted.values()}
    cobjs = {c["OBJECT_ID"]: c for c in _ets3_table(
        dump, "COMMUNICATION_OBJECT", lambda d: d["OBJECT_ID"] in obj_ids)}
    group_sizes = collections.defaultdict(collections.Counter)
    for doid, o in wanted.items():
        ia = dev_ia[o["DEVICE_ID"]]
        num = _int(o["DEVICE_OBJECT_NUMBER"])
        if num is None:
            continue
        k = obj_key(ia, num)
        c = cobjs.get(o["OBJECT_ID"], {})
        name = _clean(o.get("Name")) or _clean(c.get("OBJECT_NAME"))
        if name:
            claims.append(("object", k, "name", name, ref))
        if _clean(c.get("OBJECT_FUNCTION")):
            claims.append(("object", k, "function", _clean(c["OBJECT_FUNCTION"]), ref))
        if _clean(o.get("Description")):
            claims.append(("object", k, "description", _clean(o["Description"]), ref))
        size = otypes.get(o["DEVICE_OBJECT_TYPE"]) or otypes.get(c.get("OBJECT_TYPE"))
        if size:
            claims.append(("object", k, "size", size, ref))
        flags = "".join(ch if o.get(col) == "1" else "-" for ch, col in (
            ("C", "OBJECT_COMM"), ("R", "OBJECT_READ"), ("W", "OBJECT_WRITE"),
            ("T", "OBJECT_TRANS"), ("U", "OBJECT_UPDATE"), ("I", "OBJECT_READONINIT")))
        claims.append(("object", k, "flags", flags, ref))
        if prios.get(o["OBJECT_PRIO"]):
            claims.append(("object", k, "priority", prios[o["OBJECT_PRIO"]], ref))
        dt, sub = _clean(o.get("EIB_DATA_TYPE_CODE")), _clean(o.get("EIB_DATA_SUBTYPE_CODE"))
        if dt:
            claims.append(("object", k, "dpt", "%s.%03d" % (dt, _int(sub, 0)) if sub else dt, ref))
        if o["DEVICE_OBJECT_VISIBLE"] != "1":
            claims.append(("object", k, "visible", False, ref))
    for x in s2o:
        o = wanted.get(x["DEVICE_OBJECT_ID"])
        ga = sub_ga.get(x["SUBGROUP_ID"])
        if o is None or ga is None or _int(o["DEVICE_OBJECT_NUMBER"]) is None:
            continue
        ia = dev_ia[o["DEVICE_ID"]]
        lk = link_key(ia, o["DEVICE_OBJECT_NUMBER"], ga)
        claims.append(("link", lk, "present", True, ref))
        claims.append(("link", lk, "send", x["SEND"] == "1", ref))
        size = otypes.get(o["DEVICE_OBJECT_TYPE"])
        if size:
            group_sizes[ga][size] += 1
    for ga, cnt in group_sizes.items():
        claims.append(("group", ga, "size", cnt.most_common(1)[0][0], ref))

    # parameters the ETS user could see, with the path of headings above them
    dparams = [p for p in _ets3_table(dump, "DEVICE_PARAMETER",
                                      lambda d: d["DEVICE_ID"] in dev_ia
                                      and d["DEVICE_PARAMETER_VISIBLE"] == "1")]
    pids = {p["PARAMETER_ID"] for p in dparams}
    params = {p["PARAMETER_ID"]: p for p in _ets3_table(
        dump, "PARAMETER", lambda d: d["PROGRAM_ID"] in progs_needed)}
    types_needed = {params[i]["PARAMETER_TYPE_ID"] for i in pids if i in params}
    ptypes = {t["PARAMETER_TYPE_ID"]: t for t in _ets3_table(
        dump, "PARAMETER_TYPE", lambda d: d["PARAMETER_TYPE_ID"] in types_needed)}
    lov = collections.defaultdict(dict)
    for v in _ets3_table(dump, "PARAMETER_LIST_OF_VALUES",
                         lambda d: d["PARAMETER_TYPE_ID"] in types_needed):
        lov[v["PARAMETER_TYPE_ID"]][_clean(v["REAL_VALUE"])] = _clean(v["DISPLAYED_VALUE"])

    def ptext(p):
        return _clean(p.get("PARAMETER_DESCRIPTION")) or _clean(p.get("PARAMETER_NAME"))

    def path_of(p):
        out, seen_ids = [], set()
        cur = params.get(p.get("PAR_PARAMETER_ID"))
        while cur is not None and cur["PARAMETER_ID"] not in seen_ids and len(out) < 6:
            seen_ids.add(cur["PARAMETER_ID"])
            t = ptext(cur)
            if t:
                out.append(t)
            cur = params.get(cur.get("PAR_PARAMETER_ID"))
        return " › ".join(reversed(out[:3]))

    used = collections.defaultdict(set)
    for dp in dparams:
        p = params.get(dp["PARAMETER_ID"])
        if p is None:
            continue
        t = ptypes.get(p["PARAMETER_TYPE_ID"], {})
        if _int(t.get("ATOMIC_TYPE_NUMBER"), 0) == 0:
            continue                             # headings: no value
        raw = dp["PARAMETER_VALUE_LONG"] or dp["PARAMETER_VALUE_STRING"] or dp["PARAMETER_VALUE_DOUBLE"]
        raw = _clean(raw)
        text = lov.get(p["PARAMETER_TYPE_ID"], {}).get(raw)
        if text == "Dummy":
            continue                             # page switches without memory
        ia = dev_ia[dp["DEVICE_ID"]]
        pref = "P%s" % (_clean(p["PARAMETER_NUMBER"]) or p["PARAMETER_ID"])
        if pref in used[ia]:
            pref = "%s_%s" % (pref, p["PARAMETER_ID"])
        used[ia].add(pref)
        k = "%s:%s" % (ia, pref)
        value = _int(raw) if _int(raw) is not None else raw
        claims.append(("param", k, "name", ptext(p), ref))
        claims.append(("param", k, "value", value, ref))
        if text:
            claims.append(("param", k, "text", text, ref))
        pth = path_of(p)
        if pth:
            claims.append(("param", k, "path", pth, ref))
        addr = _int(p.get("PARAMETER_ADDRESS"), 0)
        size = _int(p.get("PARAMETER_SIZE"))
        if addr:
            claims.append(("param", k, "memory", {"address": "0x%04X" % addr,
                                                  "bit_offset": _int(p.get("PARAMETER_BITOFFSET"), 0),
                                                  "size_bits": size}, ref))
        else:
            claims.append(("param", k, "ets_only", True, ref))
        dflt = _clean(p.get("PARAMETER_DEFAULT_LONG") or p.get("PARAMETER_DEFAULT_STRING"))
        if dflt and dflt != raw:
            claims.append(("param", k, "default", _int(dflt) if _int(dflt) is not None else dflt,
                           ref))
    seen = max(stamps) if stamps else None
    return claims, seen, ref


# -- ESF ------------------------------------------------------------------------

_ESF_RE = re.compile(r"^(.*)\.([^.]*)\.(\d{1,2}/\d{1,2}/\d{1,3})$")


def _read_text(path):
    with open(path, "rb") as f:
        raw = f.read()
    for enc in ("utf-8-sig", "cp1252"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1")


def import_esf(path):
    text = _read_text(path)
    lines = text.splitlines()
    ref = "esf:%s" % os.path.basename(path)
    claims = []
    if lines and "\t" not in lines[0]:
        claims.append(("installation", "-", "name", _clean(lines[0]), ref))
        lines = lines[1:]
    for ln in lines:
        cols = ln.split("\t")
        m = _ESF_RE.match(cols[0].strip())
        if not m:
            continue
        main_name, mid_name, ga = m.group(1), m.group(2), norm_ga(m.group(3))
        if ga is None:
            continue
        a, b, _c = ga.split("/")
        claims.append(("group", ga, "present", True, ref))
        if len(cols) > 1 and _clean(cols[1]):
            claims.append(("group", ga, "name", _clean(cols[1]), ref))
        if len(cols) > 2 and _clean(cols[2]):
            claims.append(("group", ga, "eis", _clean(cols[2]), ref))
        if _clean(main_name):
            claims.append(("range", a, "name", _clean(main_name), ref))
        if _clean(mid_name):
            claims.append(("range", "%s/%s" % (a, b), "name", _clean(mid_name), ref))
    if not claims:
        raise ImportFailed("%s holds no group address lines" % path)
    return claims, ref


# -- busware stick: inventory and observation ---------------------------------

def _stick_payload(path):
    data = _load_json(path)
    if data is None:
        raise ImportFailed("%s is not JSON" % path)
    if isinstance(data, dict) and isinstance(data.get("result"), dict):
        data = data["result"]                     # a raw MCP/RPC reply
    return data


INVENTORY_ATTRS = ("present", "mask", "app_id", "serial", "order_info", "load_state")


def inventory_device_claims(ia, d, ref):
    """Claims of one device as a stick's inventory reports it."""
    claims = [("device", ia, "present", True, ref)]
    for attr, col in (("mask", "mask"), ("app_id", "application_id"),
                      ("serial", "serial_number"), ("order_info", "order_info")):
        v = d.get(col)
        if isinstance(v, str) and v.strip():
            claims.append(("device", ia, attr, v.strip().upper() if attr == "app_id"
                           else v.strip(), ref))
    lsm = d.get("lsm")
    if isinstance(lsm, list) and lsm:
        claims.append(("device", ia, "load_state",
                       " ".join("%s:%s" % (x.get("idx"), x.get("name")) for x in lsm
                                if isinstance(x, dict)), ref))
    for a in d.get("associations") or []:
        ga = norm_ga(a.get("ga"))
        num = a.get("com_obj")
        if ga is None or not isinstance(num, int):
            continue
        claims.append(("link", link_key(ia, num, ga), "present", True, ref))
    return claims


def import_inventory(path):
    """knx_inventory reply: devices with their tables.  The scanned ranges
    (`lines`) say where an absent device is absent."""
    data = _stick_payload(path)
    devices = data.get("devices") if isinstance(data, dict) else None
    if not isinstance(devices, list):
        raise ImportFailed("%s has no devices[]" % path)
    ref = "inventory:%s" % os.path.basename(path)
    claims, listed = [], set()
    for d in devices:
        ia = norm_ia(d.get("address"))
        if ia is None:
            continue
        listed.add(ia)
        claims += inventory_device_claims(ia, d, ref)
    ranges = []
    for ln in (data.get("lines") or []) if isinstance(data, dict) else []:
        lo, hi = norm_ia(ln.get("from")), norm_ia(ln.get("to"))
        if lo and hi:
            ranges.append((ia_sort(lo), ia_sort(hi)))

    def covered(ia):
        return ia in listed or any(lo <= ia_sort(ia) <= hi for lo, hi in ranges)

    def scope(kind, key, attr):
        # what an inventory states and nothing else: other bus claims (a read's
        # channel modes, object sizes from a claims file) are not its to end
        if kind == "device":
            return attr in INVENTORY_ATTRS and covered(key)
        if kind == "link":
            return covered(key.split("#", 1)[0])
        return False

    seen = norm_ts(data.get("when") or data.get("at")) if isinstance(data, dict) else None
    return claims, scope, seen, ref


def import_observe(path):
    data = _stick_payload(path)
    rows = data.get("rows") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise ImportFailed("%s has no rows[]" % path)
    ref = "observe:%s" % os.path.basename(path)
    window = data.get("window_s")
    claims = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        if r.get("ga") is not None:
            ga = norm_ga(r.get("ga"))
            if ga:
                v = {"count": r.get("count"), "window_s": window}
                if r.get("svc"):
                    v["services"] = r.get("svc")
                claims.append(("group", ga, "traffic", v, ref))
        elif r.get("address") is not None:
            ia = norm_ia(r.get("address"))
            if ia:
                claims.append(("device", ia, "sends", {"count": r.get("count"),
                                                       "window_s": window}, ref))
    return claims, ref


# -- Home Assistant knx: YAML ---------------------------------------------------

def _yaml_scalar(s):
    s = s.strip()
    if s[:1] in ("\"", "'"):                    # a comment may follow, a '#' inside stays
        end = s.find(s[0], 1)
        if end > 0:
            return s[1:end]
    if " #" in s:
        s = s.split(" #", 1)[0].rstrip()
    return s


def parse_ha_knx(text):
    """The knx: block of a Home Assistant YAML configuration -> {platform:
    [entity dicts]}.  Only the subset that block uses: a mapping of platforms to
    lists of flat mappings with scalar or list-of-scalar values."""
    out = collections.OrderedDict()
    lines = text.splitlines()
    i = 0
    base = None
    while i < len(lines):
        if re.match(r"^\s*knx:\s*(#.*)?$", lines[i]):
            base = len(lines[i]) - len(lines[i].lstrip())
            i += 1
            break
        i += 1
    if base is None:
        raise ImportFailed("no knx: block found")
    platform, ent, ent_indent, list_key, key_indent = None, None, None, None, None
    for ln in lines[i:]:
        if not ln.strip() or ln.lstrip().startswith("#"):
            continue
        ind = len(ln) - len(ln.lstrip())
        if ind <= base:
            break
        body = ln.strip()
        m = re.match(r"^([A-Za-z_][\w]*):\s*$", body)
        if m and (platform is None or ind <= plat_indent):
            platform, plat_indent, ent = m.group(1), ind, None
            out.setdefault(platform, [])
            continue
        if platform is None:
            continue
        if body.startswith("- "):
            item = body[2:].strip()
            if ent is not None and ind > ent_indent:
                # a list below one of the entity's keys; one inside a mapping nested
                # below the entity (list_key None then) is not this block's
                if list_key is not None:
                    ent[list_key].append(_yaml_scalar(item))
                continue
            ent, ent_indent, list_key = {}, ind, None
            key_indent = ind + 2 + len(body[2:]) - len(body[2:].lstrip())
            out[platform].append(ent)
            body = item
            ind = key_indent
        if ent is None:
            continue
        if ind > key_indent:
            # a mapping nested below an entity key (individual_colors: red: ...):
            # flattening it would give the entity its children's addresses
            list_key = None
            continue
        m = re.match(r"^([A-Za-z_][\w]*):\s*(.*)$", body)
        if m:
            k, v = m.group(1), m.group(2)
            if v.strip() == "":
                ent[k], list_key = [], k
            elif v.strip().startswith("["):
                ent[k] = [_yaml_scalar(x) for x in v.strip()[1:-1].split(",") if x.strip()]
                list_key = None
            else:
                ent[k], list_key = _yaml_scalar(v), None
    return out


def import_ha_yaml(path):
    plats = parse_ha_knx(_read_text(path))
    ref = "ha:%s" % os.path.basename(path)
    claims = []
    for platform, ents in plats.items():
        for e in ents:
            name = _clean(e.get("name"))
            for k, v in e.items():
                if not k.endswith("address"):
                    continue
                for ga in (v if isinstance(v, list) else [v]):
                    g = norm_ga(ga)
                    if g is None:
                        continue
                    role = k[:-len("_address")] if k.endswith("_address") else ""
                    label = name if role in ("", "state") else "%s (%s)" % (name, role)
                    if label:
                        claims.append(("group", g, "name", label, ref))
                    ent = {"platform": platform, "name": name, "role": k}
                    if "payload" in e:
                        ent["payload"] = e["payload"]
                    claims.append(("group", g, "ha_entity", ent, ref))
    if not claims:
        raise ImportFailed("no KNX entities in %s" % path)
    return claims, ref


# -- FHEM ------------------------------------------------------------------------

_FHEM_DEF_RE = re.compile(r"^define\s+(\S+)\s+(EIB|KNX)\s+(.+?)\s*$")
_FHEM_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def import_fhem(path):
    ref = "fhem:%s" % os.path.basename(path)
    claims = []
    for ln in _read_text(path).splitlines():
        m = _FHEM_DEF_RE.match(ln.strip())
        if not m:
            continue
        name, module, args = m.groups()
        for tok in args.split():
            tok = tok.split(":", 1)[0]           # KNX module: 3/2/6:dpt1
            ga = norm_ga(tok) or (fhem_hex_ga(tok) if module == "EIB" else None)
            if ga is None:
                continue
            if re.match(r"^(EIB|KNX)_[0-9a-fA-F]{4}$", name):
                claims.append(("group", ga, "fhem_autocreate", name, ref))
            else:
                claims.append(("group", ga, "name", name, ref))
    dm = _FHEM_DATE_RE.search(os.path.basename(path))
    return claims, (dm.group(1) if dm else None), ref


# -- JSON lines -------------------------------------------------------------------

def import_claims_jsonl(path):
    claims = []
    ref = "claims:%s" % os.path.basename(path)
    with open(path, "rb") as f:
        for n, raw in enumerate(f, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                d = json.loads(raw)
                kind, key = parse_ref(d["ref"]) if "ref" in d else (d["kind"], d["key"])
                attr = str(d["attr"])
                value = d["value"]
            except (ValueError, KeyError, TypeError, RefError) as e:
                raise ImportFailed("%s line %d: %s" % (path, n, e))
            if kind not in KINDS or not re.match(r"^[a-z][a-z0-9_]{0,39}$", attr):
                raise ImportFailed("%s line %d: kind/attr not allowed" % (path, n))
            claims.append((kind, key, attr, value, d.get("note") or ref))
    # the file speaks for the attributes it names, of the entries it names
    stated = {(k, key, a) for k, key, a, _v, _r in claims}

    def scope(kind, key, attr):
        return (kind, key, attr) in stated

    return claims, scope, ref


# ---------------------------------------------------------------------------
# tools
# ---------------------------------------------------------------------------

class ToolError(Exception):
    def __init__(self, error, message, **extra):
        super().__init__(message)
        self.payload = dict({"error": error, "message": message}, **extra)


def _arg_str(args, key, maxlen=200, required=False):
    v = args.get(key)
    if v is None:
        if required:
            raise ToolError("invalid_argument", "%s is required" % key)
        return None
    if not isinstance(v, str):
        raise ToolError("invalid_argument", "%s must be a string" % key)
    v = v.strip()
    if len(v) > maxlen:
        raise ToolError("invalid_argument", "%s is longer than %d characters" % (key, maxlen))
    if required and not v:
        raise ToolError("invalid_argument", "%s must not be empty" % key)
    return v


def _arg_int(args, key, default, lo, hi):
    v = args.get(key, default)
    if isinstance(v, bool) or not isinstance(v, int):
        try:
            v = int(v)
        except (TypeError, ValueError):
            raise ToolError("invalid_argument", "%s must be an integer" % key)
    if not lo <= v <= hi:
        raise ToolError("invalid_argument", "%s must be %d..%d" % (key, lo, hi))
    return v


def _arg_bool(args, key, default=False):
    v = args.get(key, default)
    if not isinstance(v, bool):
        raise ToolError("invalid_argument", "%s must be true or false" % key)
    return v


def _arg_ref(args, key="ref", required=True):
    v = _arg_str(args, key, 120, required=required)
    if v is None:
        return None
    try:
        return parse_ref(v)
    except RefError as e:
        raise ToolError("invalid_reference", str(e))


def _inst(con, args):
    v = _arg_str(args, "installation", 40)
    if v:
        try:
            return installation_row(con, v)["id"]
        except StoreError:
            pass
        rows = con.execute("SELECT id FROM installations").fetchall()
        raise ToolError("unknown_installation", "no installation %r" % v,
                        installations=[r[0] for r in rows])
    rows = con.execute("SELECT id FROM installations ORDER BY id").fetchall()
    if len(rows) == 1:
        return rows[0][0]
    if not rows:
        raise ToolError("no_installation", "this server holds no installation yet; the "
                                           "operator creates one with the init command")
    raise ToolError("choose_installation", "several installations; pass installation",
                    installations=[r[0] for r in rows])


def _size(obj):
    return len(_dumps(obj).encode("utf-8"))


def _fill(header, items, key, budget, offset=0, total=None):
    """Put as many items as fit the budget under header[key]; add next_offset."""
    out = dict(header)
    out[key] = []
    out["total"] = total if total is not None else offset + len(items)
    out["next_offset"] = None
    base = _size(out) + 40
    used = base
    for i, it in enumerate(items):
        n = _size(it) + 1
        if used + n > budget and i > 0:          # one item always: a page must move on
            out["next_offset"] = offset + i
            break
        out[key].append(it)
        used += n
    if out["next_offset"] is None and total is not None and offset + len(out[key]) < total:
        out["next_offset"] = offset + len(out[key])
    return out


def _notes_for(con, inst, refs, status=None, limit=20, cap=NOTE_CHARS):
    if not refs:
        return []
    cond = " OR ".join("(r.kind=? AND r.key=?)" for _ in refs)
    params = [inst]
    for k, key in refs:
        params += [k, key]
    q = ("SELECT DISTINCT n.* FROM notes n JOIN note_refs r ON r.note_id=n.id WHERE n.inst=? "
         "AND (%s)" % cond)
    if status:
        q += " AND n.status IN (%s)" % ",".join("?" * len(status))
        params += list(status)
    q += " ORDER BY n.id DESC LIMIT %d" % limit
    return [_note_out(con, r, cap=cap) for r in con.execute(q, params)]


def _note_out(con, r, full=True, cap=None):
    """A note; with cap, text and said cut to cap characters and "cut": true
    (proj-note action=list id=N gives it whole)."""
    about = [ref_text(x[0], x[1]) for x in con.execute(
        "SELECT kind, key FROM note_refs WHERE note_id=? ORDER BY kind, key", (r["id"],))]
    cut = False
    text = r["text"]
    if cap and len(text) > cap:
        text, cut = text[:cap] + "…", True
    e = {"id": r["id"], "kind": r["kind"], "status": r["status"], "text": text}
    if about:
        e["about"] = about
    if full:
        if r["said"]:
            said = r["said"]
            if cap and len(said) > cap:
                said, cut = said[:cap] + "…", True
            e["said"] = said
        e["updated"] = r["updated"][:16]
    if cut:
        e["cut"] = True
    return e


def _group_rows(con, inst):
    return by_entity(load_claims(con, inst, kind="group"))


def _links_current(con, inst, prefix=None):
    """{link_key: [rows]} of current link claims."""
    return by_entity(load_claims(con, inst, kind="link",
                                 keys=(prefix + "*") if prefix else None))


# -- proj-brief ---------------------------------------------------------------------

def tool_brief(store, args, budget):
    with store.reader() as con:
        inst = _inst(con, args)
        row = installation_row(con, inst)
        prec = precedence_of(con, inst)
        res = resolve(load_claims(con, inst, kind="installation", keys=["-"]), prec)
        counts = {}
        for kind in ("device", "group", "object", "link", "param"):
            counts[kind + "s"] = con.execute(
                "SELECT count(DISTINCT key) FROM claims WHERE inst=? AND kind=? AND "
                "ended_at IS NULL", (inst, kind)).fetchone()[0]
        srcs = []
        for r in con.execute("SELECT source, format, ref, seen, at, stats FROM imports i WHERE "
                             "inst=? AND id=(SELECT max(id) FROM imports j WHERE j.inst=i.inst "
                             "AND j.source=i.source AND j.format=i.format) ORDER BY source",
                             (inst,)):
            st = json.loads(r["stats"] or "{}")
            e = {"source": r["source"], "format": r["format"], "ref": r["ref"],
                 "as_of": (r["seen"] or "")[:10], "imported": r["at"][:10],
                 "claims": st.get("asserted")}
            if st.get("date_from") == "file":
                # only the file's modification time is known, not when the data was true
                e["as_of"] = None
                e["file_date"] = (r["seen"] or "")[:10]
            srcs.append(e)
        # the record's own name and description are not decisions about the installation
        user_n = con.execute("SELECT count(*) FROM claims WHERE inst=? AND source='user' AND "
                             "ended_at IS NULL AND NOT (kind='installation' AND attr IN "
                             "('name', 'description', 'bus_access', 'stick_id'))", (inst,)).fetchone()[0]
        rules = [_note_out(con, r, full=False) for r in con.execute(
            "SELECT * FROM notes WHERE inst=? AND kind='rule' AND status IN ('open','confirmed') "
            "ORDER BY id", (inst,))]
        open_n = {}
        open_items = []
        for kind in ("finding", "todo"):
            rs = con.execute("SELECT * FROM notes WHERE inst=? AND kind=? AND status='open' "
                             "ORDER BY id DESC", (inst, kind)).fetchall()
            open_n[kind + "s"] = len(rs)
            open_items += [_note_out(con, r, full=False) for r in rs[:BRIEF_OPEN_MAX]]
        pending = _pending_count(con, inst)
    desc = value_of(res, "description") or ""
    access = value_of(res, "bus_access")
    out = {
        "installation": {"id": inst, "name": value_of(res, "name") or row["name"]},
        "description": desc[:4000] + ("…" if len(desc) > 4000 else ""),
        "bus_access": access or "not recorded: which stick or interface reaches this bus "
                                "is unknown to this record",
        "stick_id": value_of(res, "stick_id"),
        "rules": rules,
        "how_to": [
            "Values come with a source; names may differ between sources. proj-group / "
            "proj-device show every source. Precedence decides which value counts.",
            "What the operator decided: proj-set with said = their words. What you "
            "inferred: proj-note kind=finding (status open) -- never write an inference "
            "as a fact.",
            "This record never changes the bus. Changing a device is a separate step with "
            "the stick's own tools, after the operator agreed.",
            "A stick's tools act on the bus that stick is wired to, which need not be this "
            "installation's. Before reading or writing a device of it, compare the stick's "
            "describe (stick_id, own address) with bus_access and look for this "
            "installation's senders in its passive observe list.",
            "To bring the record up to date with a device, proj-read-device reads it through "
            "the stick named by stick_id (reads only, but real bus traffic: only when the "
            "operator asked).",
            "Draw the installation from proj-topology (format=mermaid gives a ready graph)."],
        "precedence": prec,
        "counts": counts,
        "sources": srcs,
        "decisions_by_user": user_n,
        "open": dict(open_n, items=open_items),
        "pending_changes": pending,
    }
    op = out["open"]
    # over budget: shorten the open items first, then drop the oldest findings
    if _size(out) > budget:
        for it in op["items"]:
            if len(it["text"]) > BRIEF_ITEM_CHARS:
                it["text"] = it["text"][:BRIEF_ITEM_CHARS] + "…"
                op["texts_cut"] = True
    while _size(out) > budget and any(it["kind"] == "finding" for it in op["items"]):
        del op["items"][max(i for i, it in enumerate(op["items"]) if it["kind"] == "finding")]
    if _size(out) > budget:
        out["description"] = out["description"][:1500] + "…"
    if _size(out) > budget:
        for r in out["rules"]:
            if len(r["text"]) > RULE_CHARS:
                r["text"] = r["text"][:RULE_CHARS] + "…"
                r["cut"] = True
                out["rules_cut"] = True
    if _size(out) > budget:
        out["rules"] = out["rules"][:10]
        out["rules_truncated"] = True
    if out.get("rules_cut") or out.get("rules_truncated"):
        out["rules_rest"] = "proj-note action=list kind=rule gives every rule in full"
    shown = len(op["items"])
    if shown < sum(open_n.values()):
        op["items_shown"] = shown
    if shown < sum(open_n.values()) or op.get("texts_cut"):
        op["rest"] = "proj-note action=list status=open gives every open item in full"
    return _dumps(out)


def _pending_count(con, inst):
    d = _diff(con, inst, "user", "bus", limit=0)
    return sum(v["total"] for v in d.values() if isinstance(v, dict) and "total" in v)


# -- proj-find ----------------------------------------------------------------------


def load_find_claims(con, inst):
    """The current claims proj-find searches, read from claims_find_v3 alone."""
    out = []
    for r in con.execute("SELECT " + _CLAIM_COLS + " FROM claims WHERE inst=? AND attr IN ("
                         + _FIND_ATTRS_SQL + ") AND ended_at IS NULL", (inst,)):
        d = dict(r)
        d["value"] = json.loads(d["value"])
        out.append(d)
    return out


def tool_find(store, args, budget):
    query = _arg_str(args, "query", 120, required=True)
    kinds = args.get("kinds")
    if kinds is not None and (not isinstance(kinds, list)
                              or any(k not in KINDS + ("note",) for k in kinds)):
        raise ToolError("invalid_argument", "kinds is a list of %s" % ", ".join(KINDS))
    offset = _arg_int(args, "offset", 0, 0, 100000)
    words = [w for w in _fold(query).split() if w]
    with store.reader() as con:
        inst = _inst(con, args)
        prec = precedence_of(con, inst)
        try:
            k, key = parse_ref(query)
            exact = load_claims(con, inst, kind=k, keys=[key])
        except RefError:
            exact = []
        rows = load_find_claims(con, inst)
        ents = by_entity(r for r in rows if kinds is None or r["kind"] in kinds)
        hits = []
        if exact:
            ent = by_entity(exact)
            for (k, key), rs in ent.items():
                hits.append((0, k, key, rs))
        for (k, key), rs in ents.items():
            hay = _fold(" ".join("%s" % r["value"] for r in rs) + " " + key)
            if all(w in hay for w in words) and not any(h[1] == k and h[2] == key for h in hits):
                hits.append((1 + KINDS.index(k), k, key, rs))
        notes = []
        if kinds is None or "note" in kinds:
            for r in con.execute("SELECT * FROM notes WHERE inst=? ORDER BY id DESC", (inst,)):
                if all(w in _fold(r["text"] + " " + (r["said"] or "")) for w in words):
                    notes.append(_note_out(con, r, full=False, cap=NOTE_CHARS))
    hits.sort(key=lambda h: (h[0], key_sort(h[1], h[2])))
    items = []
    for _rank, k, key, rs in hits:
        res = resolve(rs, prec)
        e = {"ref": ref_text(k, key), "kind": k}
        name = device_label(res) if k == "device" else value_of(res, "name")
        if name:
            e["name"] = name
        matched = sorted({r["attr"] for r in rs
                          if any(w in _fold("%s" % r["value"]) for w in words)})
        if matched:
            e["matched"] = matched
        items.append(e)
    window = items[offset:]
    out = _fill({"query": query}, window, "results", budget - 600, offset, len(items))
    if notes:
        out["notes"] = notes[:8]
        if _size(out) > budget:
            out["notes"] = notes[:2]
    return _dumps(out)


# -- proj-group -----------------------------------------------------------------------

def _range_filter(text):
    """'3' or '3/*' -> main group, '3/1' or '3/1/*' -> middle group,
    '*' -> all.  -> predicate on a group address."""
    t = text.strip().rstrip("*").rstrip("/")
    if t == "":
        return lambda ga: True
    parts = t.split("/")
    if not all(_NUM_RE.match(p) for p in parts) or len(parts) > 2:
        return None
    nums = [int(p) for p in parts]
    return lambda ga: [int(x) for x in ga.split("/")[:len(nums)]] == nums


def _link_index(con, inst, prec, ga_set=None, ia=None):
    """-> {ga: [link entries]}, {ia: [link entries]} for current links."""
    rows = load_claims(con, inst, kind="link", keys=(ia + "#*") if ia else None)
    ents = by_entity(rows)
    by_ga, by_dev = collections.defaultdict(list), collections.defaultdict(list)
    for (_k, key), rs in ents.items():
        try:
            dev, num, ga = split_link(key)
        except ValueError:
            continue
        if ga_set is not None and ga not in ga_set:
            continue
        res = resolve(rs, prec)
        present = sources_saying(rs, "present", True)
        absent = sources_saying(rs, "present", False)
        if not present:
            continue
        e = {"device": dev, "object": num, "ga": ga, "sources": present}
        if absent:
            e["removed_by"] = absent
        if value_of(res, "send"):
            e["send"] = True
        by_ga[ga].append(e)
        by_dev[dev].append(e)
    return by_ga, by_dev


def _object_names(con, inst, prec, keys):
    ents = by_entity(load_claims(con, inst, kind="object", keys=list(keys),
                                 attrs=("name", "function", "size", "flags", "slot_name")))
    return {key: resolve(rs, prec) for (_k, key), rs in ents.items()}


def tool_group(store, args, budget):
    text = _arg_str(args, "ga", 40, required=True)
    offset = _arg_int(args, "offset", 0, 0, 100000)
    with store.reader() as con:
        inst = _inst(con, args)
        prec = precedence_of(con, inst)
        single = norm_ga(text)
        if single:
            return _dumps(_group_detail(con, inst, prec, single, budget))
        pred = _range_filter(text)
        if pred is None:
            raise ToolError("invalid_argument", "ga is a group address (3/1/7) or a range "
                                                "(3, 3/1, 3/*, *)")
        groups = {key: rs for (_k, key), rs in _group_rows(con, inst).items() if pred(key)}
        by_ga, _ = _link_index(con, inst, prec)
        for ga in by_ga:
            if pred(ga):
                groups.setdefault(ga, [])
        ranges = {key: resolve(rs, prec) for (_k, key), rs in
                  by_entity(load_claims(con, inst, kind="range")).items()}
    items = []
    for ga in sorted(groups, key=ga_int):
        res = resolve(groups[ga], prec)
        e = {"ga": ga}
        if value_of(res, "name"):
            e["name"] = value_of(res, "name")
            if res["name"].get("differs"):
                e["name_differs"] = res["name"]["differs"]
        for attr in ("dpt", "size", "eis"):
            if value_of(res, attr):
                e[attr] = value_of(res, attr)
                break
        devs = sorted({x["device"] for x in by_ga.get(ga, [])}, key=ia_sort)
        if devs:
            e["devices"] = devs
        src = sorted({r["source"] for r in groups[ga] if r["ended_at"] is None} |
                     {s for x in by_ga.get(ga, []) for s in x["sources"]})
        e["sources"] = src
        if value_of(res, "traffic"):
            e["seen_on_bus"] = True
        items.append(e)
    rng = {}
    for ga in groups:
        a, b, _c = ga.split("/")
        for k in (a, "%s/%s" % (a, b)):
            if k in ranges and value_of(ranges[k], "name"):
                rng[k] = value_of(ranges[k], "name")
    header = {"range": text, "range_names": rng}
    return _dumps(_fill(header, items[offset:], "groups", budget, offset, len(items)))


def _group_detail(con, inst, prec, ga, budget):
    rows = load_claims(con, inst, kind="group", keys=[ga], ended=True)
    res = resolve(rows, prec)
    by_ga, _ = _link_index(con, inst, prec, {ga})
    links = by_ga.get(ga, [])
    names = _object_names(con, inst, prec, {obj_key(x["device"], x["object"]) for x in links})
    devs = by_entity(load_claims(con, inst, kind="device",
                                 keys=list({x["device"] for x in links}),
                                 attrs=("name", "description", "product")))
    dev_names = {key: device_label(resolve(rs, prec)) for (_k, key), rs in devs.items()}
    out = {"ga": ga}
    for attr in ("name", "description", "dpt", "size", "eis", "central", "passthrough",
                 "safety", "comment"):
        if attr in res:
            out[attr] = res[attr]["value"]
            if attr == "name":
                out["name_source"] = res[attr]["source"]
    if not rows and not links:
        out["known"] = False
    a, b, _c = ga.split("/")
    rr = by_entity(load_claims(con, inst, kind="range", keys=[a, "%s/%s" % (a, b)]))
    out["range"] = " / ".join(
        value_of(resolve(rr.get(("range", k), []), prec), "name") or k
        for k in (a, "%s/%s" % (a, b)))
    ents = [r["value"] for r in rows if r["attr"] == "ha_entity" and r["ended_at"] is None]
    if ents:
        out["ha_entities"] = ents
    if "traffic" in res:
        out["traffic"] = res["traffic"]["value"]
        out["traffic_as_of"] = next((r["last_seen"][:10] for r in rows
                                     if r["attr"] == "traffic" and r["ended_at"] is None), None)
    lk = []
    for x in sorted(links, key=lambda x: (ia_sort(x["device"]), x["object"])):
        e = dict(x)
        e["device_name"] = dev_names.get(x["device"], "")
        o = names.get(obj_key(x["device"], x["object"]), {})
        for attr in ("name", "function", "size", "flags"):
            if value_of(o, attr):
                e["object_" + attr] = value_of(o, attr)
        if not value_of(o, "name") and value_of(o, "slot_name"):
            e["object_slot_name"] = value_of(o, "slot_name")
        lk.append(e)
    out["links"] = lk
    out["claims"] = [c for c in claim_history(rows) if c["attr"] not in ("present",)]
    out["notes"] = _notes_for(con, inst, [("group", ga)])
    if _size(out) > budget:
        out["claims"] = [c for c in out["claims"] if c["attr"] == "name"]
        out["claims_reduced"] = True
    if _size(out) > budget and len(out["notes"]) > 5:
        out["notes"] = out["notes"][:5]
        out["notes_truncated"] = True
    if _size(out) > budget:
        out["links"] = out["links"][:30]
        out["links_truncated"] = True
    return out


# -- proj-device --------------------------------------------------------------------

_DEV_ATTRS = ("name", "description", "location", "role", "comment", "product",
              "order_number", "manufacturer", "app_name", "app_id", "mask", "serial",
              "order_info", "load_state", "channel_modes", "last_modified", "last_download",
              "sends")


def tool_device(store, args, budget):
    ia = norm_ia(_arg_str(args, "address", 20, required=True))
    if ia is None:
        raise ToolError("invalid_argument", "address is an individual address, e.g. 1.2.3")
    section = _arg_str(args, "section", 20) or "summary"
    if section not in ("summary", "objects", "params"):
        raise ToolError("invalid_argument", "section is summary, objects or params")
    query = _arg_str(args, "query", 120)
    offset = _arg_int(args, "offset", 0, 0, 100000)
    with store.reader() as con:
        inst = _inst(con, args)
        prec = precedence_of(con, inst)
        rows = load_claims(con, inst, kind="device", keys=[ia], ended=True)
        if section == "params":
            return _dumps(_device_params(con, inst, prec, ia, query, offset, budget))
        _by_ga, by_dev = _link_index(con, inst, prec, ia=ia)
        links = by_dev.get(ia, [])
        objs = by_entity(load_claims(con, inst, kind="object", keys=ia + "#*"))
        if not rows and not links and not objs:
            raise ToolError("unknown_device", "%s is not in the record" % ia)
        res = resolve(rows, prec)
        line = ia.rsplit(".", 1)[0]
        lres = resolve(load_claims(con, inst, kind="line", keys=[line]), prec)
        gnames = {key: value_of(resolve(rs, prec), "name") for (_k, key), rs in
                  by_entity(load_claims(con, inst, kind="group",
                                        keys=list({x["ga"] for x in links}),
                                        attrs=("name",))).items()}
        n_params = con.execute("SELECT count(DISTINCT key) FROM claims WHERE inst=? AND "
                               "kind='param' AND key >= ? AND key < ? AND ended_at IS NULL",
                               (inst, ia + ":", ia + ";")).fetchone()[0]
        notes = _notes_for(con, inst, [("device", ia)] +
                           [("object", obj_key(ia, x["object"])) for x in links][:200])
    if section == "objects":
        return _dumps(_device_objects(ia, prec, objs, links, gnames, query, offset, budget))
    out = {"address": ia, "line": line}
    if value_of(lres, "name"):
        out["line_name"] = value_of(lres, "name")
    out["present"] = {"says_yes": sources_saying(rows, "present", True),
                      "says_no": sources_saying(rows, "present", False)}
    for attr in _DEV_ATTRS:
        if attr in res:
            e = res[attr]
            out[attr] = e["value"]
            if e.get("differs"):
                out.setdefault("differs", {})[attr] = {
                    r["source"]: r["value"] for r in rows
                    if r["attr"] == attr and r["ended_at"] is None}
    per_obj = collections.defaultdict(list)
    for x in links:
        per_obj[x["object"]].append(x)
    lk = []
    for num in sorted(per_obj):
        o = resolve(objs.get(("object", obj_key(ia, num)), []), prec)
        e = {"object": num}
        if value_of(o, "name"):
            e["name"] = value_of(o, "name")
        if value_of(o, "function"):
            e["function"] = value_of(o, "function")
        if not value_of(o, "name") and value_of(o, "slot_name"):
            e["slot_name"] = value_of(o, "slot_name")
        e["groups"] = []
        for x in sorted(per_obj[num], key=lambda x: ga_int(x["ga"])):
            g = {"ga": x["ga"], "sources": x["sources"]}
            if gnames.get(x["ga"]):
                g["name"] = gnames[x["ga"]]
            if x.get("send"):
                g["send"] = True
            e["groups"].append(g)
        lk.append(e)
    out["linked_objects"] = lk
    out["counts"] = {"objects_known": len(objs), "params_known": n_params}
    out["notes"] = notes
    out["more"] = "section=objects for every com object, section=params (query=...) for parameters"
    if _size(out) > budget:
        for e in out["linked_objects"]:
            for g in e["groups"]:
                g.pop("name", None)
        out["linked_objects_reduced"] = True
    if _size(out) > budget and len(out["notes"]) > 5:
        out["notes"] = out["notes"][:5]
        out["notes_truncated"] = True
    if _size(out) > budget:
        out["linked_objects"] = out["linked_objects"][:20]
        out["linked_objects_truncated"] = True
    return _dumps(out)


def _device_objects(ia, prec, objs, links, gnames, query, offset, budget):
    per_obj = collections.defaultdict(list)
    for x in links:
        per_obj[x["object"]].append(x)
    nums = set(per_obj)
    for (_k, key) in objs:
        nums.add(int(key.split("#", 1)[1]))
    words = [w for w in _fold(query or "").split() if w]
    items = []
    for num in sorted(nums):
        o = resolve(objs.get(("object", obj_key(ia, num)), []), prec)
        e = {"object": num}
        for attr in ("name", "function", "size", "flags", "dpt", "priority", "description",
                     "slot_name"):
            if value_of(o, attr):
                e[attr] = value_of(o, attr)
        if value_of(o, "visible") is False:
            e["visible"] = False
        gs = [x["ga"] + ("*" if x.get("send") else "") for x in
              sorted(per_obj.get(num, []), key=lambda x: ga_int(x["ga"]))]
        if gs:
            e["groups"] = gs
        if words and not all(w in _fold(_dumps(e)) for w in words):
            continue
        items.append(e)
    header = {"address": ia, "section": "objects",
              "legend": "groups: '*' marks the sending group address; flags C R W T U I, "
                        "'-' = off; slot_name = name of the slot in the product data, "
                        "which the channel's mode may not use that way"}
    return _fill(header, items[offset:], "objects", budget, offset, len(items))


def _device_params(con, inst, prec, ia, query, offset, budget):
    ents = by_entity(load_claims(con, inst, kind="param", keys=ia + ":*"))
    words = [w for w in _fold(query or "").split() if w]
    items = []
    for (_k, key), rs in sorted(ents.items(), key=lambda kv: key_sort("param", kv[0][1])):
        res = resolve(rs, prec)
        e = {"ref": key.split(":", 1)[1]}
        for attr in ("path", "name", "value", "text", "default", "memory", "ets_only"):
            if attr in res:
                e[attr] = res[attr]["value"]
        if res.get("value", {}).get("differs"):
            e["value_by_source"] = {r["source"]: r["value"] for r in rs if r["attr"] == "value"}
        if words and not all(w in _fold(_dumps(e)) for w in words):
            continue
        items.append(e)
    header = {"address": ia, "section": "params",
              "legend": "value = raw value, text = its meaning; default only where the "
                        "value differs; ets_only = stored in the ETS project, not in the "
                        "device; memory = where the device keeps it"}
    return _fill(header, items[offset:], "params", budget, offset, len(items))


# -- proj-topology ----------------------------------------------------------------------

def _mm_id(prefix, key):
    return prefix + re.sub(r"[^0-9A-Za-z]", "_", key)


def _mm_text(s):
    return str(s).replace('"', "'").replace("<", "‹").replace(">", "›")


def tool_topology(store, args, budget):
    view = _arg_str(args, "view", 20) or "physical"
    fmt = _arg_str(args, "format", 20) or "json"
    rng = _arg_str(args, "range", 20)
    if view not in ("physical", "functional") or fmt not in ("json", "mermaid"):
        raise ToolError("invalid_argument", "view is physical or functional, format json or "
                                            "mermaid")
    with store.reader() as con:
        inst = _inst(con, args)
        prec = precedence_of(con, inst)
        if view == "physical":
            data = _physical(con, inst, prec)
        else:
            pred = _range_filter(rng or "*")
            if pred is None:
                raise ToolError("invalid_argument", "range is 3 or 3/1")
            data = _functional(con, inst, prec, pred)
    if fmt == "mermaid":
        text = _mermaid_physical(data) if view == "physical" else _mermaid_functional(data)
        out = {"view": view, "format": "mermaid", "mermaid": text,
               "legend": "dashed = known only from a project file, not confirmed on the bus"}
        if _size(out) > budget:
            out["mermaid"] = text[:budget - 400]
            out["truncated"] = True
            out["hint"] = "narrow it: view=functional range=3/1"
        return _dumps(out)
    out = {"view": view, "tree": data}
    if _size(out) > budget:
        out = {"view": view, "truncated": True, "hint": "use format=mermaid or a range",
               "tree": data[:3]}
    return _dumps(out)


def _physical(con, inst, prec):
    devs = by_entity(load_claims(con, inst, kind="device"))
    lines = {key: resolve(rs, prec) for (_k, key), rs in
             by_entity(load_claims(con, inst, kind="line")).items()}
    areas = {key: resolve(rs, prec) for (_k, key), rs in
             by_entity(load_claims(con, inst, kind="area")).items()}
    tree = collections.defaultdict(lambda: collections.defaultdict(list))
    for (_k, ia), rs in devs.items():
        yes = sources_saying(rs, "present", True)
        if not yes:
            continue
        res = resolve(rs, prec)
        a, l, _d = ia.split(".")
        e = {"address": ia, "name": device_label(res), "sources": yes}
        for attr in ("product", "order_number", "app_id"):
            if value_of(res, attr):
                e[attr] = value_of(res, attr)
        no = sources_saying(rs, "present", False)
        if no:
            e["absent_per"] = no
        tree[a]["%s.%s" % (a, l)].append(e)
    for lk in lines:
        a = lk.split(".")[0]
        tree[a].setdefault(lk, [])
    out = []
    for a in sorted(tree, key=int):
        ae = {"area": a}
        if a in areas and value_of(areas[a], "name"):
            ae["name"] = value_of(areas[a], "name")
        ae["lines"] = []
        for lk in sorted(tree[a], key=lambda k: tuple(int(x) for x in k.split("."))):
            le = {"line": lk}
            if lk in lines:
                for attr in ("name", "medium"):
                    if value_of(lines[lk], attr):
                        le[attr] = value_of(lines[lk], attr)
            le["devices"] = sorted(tree[a][lk], key=lambda e: ia_sort(e["address"]))
            if le["devices"] or le.get("name"):
                ae["lines"].append(le)
        out.append(ae)
    return out


def _mermaid_physical(data):
    out = ["flowchart TD"]
    dashed = []
    for a in data:
        aid = _mm_id("A", a["area"])
        out.append('  %s["Area %s%s"]' % (aid, a["area"],
                                         (" · " + _mm_text(a["name"])) if a.get("name") else ""))
        for l in a["lines"]:
            lid = _mm_id("L", l["line"])
            label = "Line %s%s%s" % (l["line"], (" · " + _mm_text(l["name"])) if l.get("name") else "",
                                     (" (%s)" % l["medium"]) if l.get("medium") else "")
            out.append('  %s["%s"]' % (lid, label))
            out.append("  %s --> %s" % (aid, lid))
            for d in l["devices"]:
                did = _mm_id("D", d["address"])
                sub = d.get("product") or d.get("app_id") or ""
                out.append('  %s["%s%s%s"]' % (did, d["address"],
                                               ("<br/>" + _mm_text(d["name"])) if d.get("name") else "",
                                               ("<br/><small>" + _mm_text(sub) + "</small>")
                                               if sub and sub != d.get("name") else ""))
                out.append("  %s --> %s" % (lid, did))
                if "bus" not in d["sources"] and "user" not in d["sources"]:
                    dashed.append(did)
    if dashed:
        out.append("  classDef unconfirmed stroke-dasharray: 5 5")
        out.append("  class %s unconfirmed" % ",".join(dashed))
    return "\n".join(out)


def _functional(con, inst, prec, pred):
    groups = {key: rs for (_k, key), rs in _group_rows(con, inst).items() if pred(key)}
    by_ga, _ = _link_index(con, inst, prec)
    for ga in list(by_ga):
        if pred(ga):
            groups.setdefault(ga, [])
    ranges = {key: resolve(rs, prec) for (_k, key), rs in
              by_entity(load_claims(con, inst, kind="range")).items()}
    tree = collections.defaultdict(lambda: collections.defaultdict(list))
    for ga in sorted(groups, key=ga_int):
        res = resolve(groups[ga], prec)
        a, b, _c = ga.split("/")
        links = by_ga.get(ga, [])
        e = {"ga": ga, "name": value_of(res, "name") or ""}
        devs = sorted({x["device"] for x in links}, key=ia_sort)
        if devs:
            e["devices"] = devs
        if not any("bus" in x["sources"] for x in links):
            e["no_bus_link"] = True
        tree[a]["%s/%s" % (a, b)].append(e)
    out = []
    for a in sorted(tree, key=int):
        me = {"main": a, "name": value_of(ranges.get(a, {}), "name") or "", "middles": []}
        for mk in sorted(tree[a], key=lambda k: int(k.split("/")[1])):
            me["middles"].append({"middle": mk, "name": value_of(ranges.get(mk, {}), "name") or "",
                                  "groups": tree[a][mk]})
        out.append(me)
    return out


def _mermaid_functional(data):
    out = ["flowchart LR"]
    devices = set()
    for m in data:
        for mid in m["middles"]:
            sid = _mm_id("M", mid["middle"])
            out.append('  subgraph %s["%s"]' % (sid, (mid["middle"] + " " + _mm_text(mid["name"])).strip()))
            for g in mid["groups"]:
                out.append('    %s["%s"]' % (_mm_id("G", g["ga"]), (g["ga"] + " " + _mm_text(g["name"])).strip()))
            out.append("  end")
            for g in mid["groups"]:
                for d in g.get("devices", []):
                    devices.add(d)
                    out.append("  %s --- %s" % (_mm_id("G", g["ga"]), _mm_id("D", d)))
    for d in sorted(devices, key=ia_sort):
        out.append('  %s(["%s"])' % (_mm_id("D", d), d))
    return "\n".join(out)


# -- proj-diff -------------------------------------------------------------------------

def _present_map(con, inst, kind, source):
    out = {}
    for r in load_claims(con, inst, kind=kind, attrs=("present",), source=source):
        out[r["key"]] = r["value"]
    return out


def _diff(con, inst, a, b, limit=40):
    """What source a states and source b does not (and the reverse)."""
    out = {}
    da, db = _present_map(con, inst, "device", a), _present_map(con, inst, "device", b)
    only_a = sorted([k for k, v in da.items() if v and not db.get(k)], key=ia_sort)
    only_b = sorted([k for k, v in db.items() if v and not da.get(k)], key=ia_sort)
    removed = sorted([k for k, v in da.items() if v is False and db.get(k)], key=ia_sort)
    out["devices_only_in_" + a] = {"total": len(only_a), "items": only_a[:limit]}
    if a != "user":
        out["devices_only_in_" + b] = {"total": len(only_b), "items": only_b[:limit]}
    if removed:
        out["devices_to_remove"] = {"total": len(removed), "items": removed[:limit]}
    both = {k for k, v in da.items() if v and db.get(k)}
    attr_diff = []
    ca = by_entity(load_claims(con, inst, kind="device", keys=list(both),
                               attrs=("app_id", "mask", "serial"), source=a))
    cb = by_entity(load_claims(con, inst, kind="device", keys=list(both),
                               attrs=("app_id", "mask", "serial"), source=b))
    for ent in sorted(set(ca) & set(cb), key=lambda e: ia_sort(e[1])):
        va = {r["attr"]: r["value"] for r in ca[ent]}
        vb = {r["attr"]: r["value"] for r in cb[ent]}
        for attr in sorted(set(va) & set(vb)):
            if str(va[attr]).upper() != str(vb[attr]).upper():
                attr_diff.append({"device": ent[1], "attr": attr, a: va[attr], b: vb[attr]})
    out["device_attributes"] = {"total": len(attr_diff), "items": attr_diff[:limit]}
    la, lb = _present_map(con, inst, "link", a), _present_map(con, inst, "link", b)
    if a == "user":
        devs = None
    else:
        devs = both
    l_only_a, l_only_b, l_remove = [], [], []
    for k, v in la.items():
        dev = k.split("#", 1)[0]
        if devs is not None and dev not in devs:
            continue
        if v and not lb.get(k):
            l_only_a.append(k)
        if v is False and lb.get(k):
            l_remove.append(k)
    for k, v in lb.items():
        dev = k.split("#", 1)[0]
        if devs is not None and dev not in devs:
            continue
        if v and not la.get(k) and a != "user":
            l_only_b.append(k)
    srt = lambda k: key_sort("link", k)
    out["links_only_in_" + a] = {"total": len(l_only_a), "items": sorted(l_only_a, key=srt)[:limit]}
    if a != "user":
        out["links_only_in_" + b] = {"total": len(l_only_b),
                                     "items": sorted(l_only_b, key=srt)[:limit]}
    if l_remove:
        out["links_to_remove"] = {"total": len(l_remove), "items": sorted(l_remove, key=srt)[:limit]}
    if a == "user":
        pv = []
        ua = load_claims(con, inst, kind="param", attrs=("value",), source="user")
        ub = {r["key"]: r["value"] for r in
              load_claims(con, inst, kind="param", keys=[r["key"] for r in ua],
                          attrs=("value",), source=b)}
        prec = [x for x in precedence_of(con, inst) if x != "user"]
        for r in ua:
            if ub.get(r["key"]) != r["value"]:
                e = {"param": r["key"], "want": r["value"], b: ub.get(r["key"])}
                other = resolve(load_claims(con, inst, kind="param", keys=[r["key"]],
                                            attrs=("value", "name", "path")), prec)
                for attr in ("name", "path"):
                    if value_of(other, attr):
                        e[attr] = value_of(other, attr)
                if "value" in other:
                    e["last_known"] = {"value": other["value"]["value"],
                                       "source": other["value"]["source"]}
                pv.append(e)
        out["params_to_change"] = {"total": len(pv), "items": pv[:limit]}
    return out


def tool_diff(store, args, budget):
    a = _arg_str(args, "a", 20) or "ets"
    b = _arg_str(args, "b", 20) or "bus"
    if a not in SOURCES or b not in SOURCES or a == b:
        raise ToolError("invalid_argument", "a and b are two different sources: %s"
                        % ", ".join(DEFAULT_PRECEDENCE))
    with store.reader() as con:
        inst = _inst(con, args)
        out = {"a": a, "b": b}
        if a == "user":
            out["meaning"] = ("what the operator decided (user) and the devices do not hold "
                              "yet (bus): the work left to do on the installation")
        out.update(_diff(con, inst, a, b))
        out["note"] = ("compares device presence, app_id/mask/serial, links (device#object>ga) "
                       "of devices both sources know; a link absent in one source may also "
                       "mean that source never read that device")
    if _size(out) > budget:
        out = {k: ({"total": v["total"], "items": v["items"][:8]} if isinstance(v, dict)
                   and "items" in v else v) for k, v in out.items()}
        out["truncated"] = True
    return _dumps(out)


# -- proj-history -------------------------------------------------------------------------

def tool_history(store, args, budget):
    ref = _arg_ref(args, "ref", required=False)
    offset = _arg_int(args, "offset", 0, 0, 10000000)
    with store.reader() as con:
        inst = _inst(con, args)
        if ref is None:
            total = con.execute("SELECT count(*) FROM journal WHERE inst=?", (inst,)).fetchone()[0]
            js = [dict(r) for r in con.execute(
                "SELECT at, by, tool, args, result FROM journal WHERE inst=? ORDER BY id DESC "
                "LIMIT 40 OFFSET ?", (inst, offset))]
            for j in js:
                j["args"] = _journal_value(j["args"])
                j["result"] = _journal_value(j["result"])
            return _dumps(_fill({"installation": inst}, js, "journal", budget, offset, total))
        kind, key = ref
        rows = load_claims(con, inst, kind=kind, keys=[key], ended=True)
        # the entries naming this reference as a JSON string ("ref", "address",
        # "about"): 1.2.3 is not 11.2.3 or 1.2.30, and _ in a key is no wildcard
        needle = _dumps(ref_text(kind, key))
        like = "%" + needle.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        js = [dict(r) for r in con.execute(
            "SELECT at, by, tool, args FROM journal WHERE inst=? AND args LIKE ? ESCAPE '\\' "
            "ORDER BY id DESC LIMIT 20", (inst, like))]
        for j in js:
            j["args"] = _shorten(_journal_value(j["args"]))
        notes = _notes_for(con, inst, [ref], limit=40)
    out = {"ref": ref_text(kind, key), "notes": notes, "journal": js}
    # the claims are what this view pages; notes and journal must leave them room
    while _size(out) > budget // 2 and (js or notes):
        if js:
            js.pop()
            out["journal_truncated"] = True
        else:
            notes.pop()
            out["notes_truncated"] = True
    hist = claim_history(rows)
    return _dumps(_fill(out, hist[offset:], "claims", budget, offset, len(hist)))


# -- proj-note ---------------------------------------------------------------------------

def _caller(args):
    return args.get("_by") or "mcp"


def tool_note(store, args, budget):
    action = _arg_str(args, "action", 20) or "add"
    if action not in ("add", "update", "list"):
        raise ToolError("invalid_argument", "action is add, update or list")
    if action == "list":
        with store.reader() as con:
            inst = _inst(con, args)
            kind = _arg_str(args, "kind", 20)
            status = _arg_str(args, "status", 20)
            q, p = "SELECT * FROM notes WHERE inst=?", [inst]
            if kind:
                q += " AND kind=?"
                p.append(kind)
            if status:
                q += " AND status=?"
                p.append(status)
            if args.get("id") is not None:
                q += " AND id=?"
                p.append(_arg_int(args, "id", 0, 1, 1 << 40))
            notes = [_note_out(con, r) for r in con.execute(q + " ORDER BY id DESC", p)]
            offset = _arg_int(args, "offset", 0, 0, 100000)
        return _dumps(_fill({"installation": inst}, notes[offset:], "notes", budget,
                            offset, len(notes)))
    with store.writer() as con:
        inst = _inst(con, args)
        about = args.get("about") or []
        if isinstance(about, str):
            about = [about]
        if not isinstance(about, list) or len(about) > 40:
            raise ToolError("invalid_argument", "about is a list of references (≤ 40)")
        refs = []
        for a in about:
            try:
                refs.append(parse_ref(a))
            except RefError as e:
                raise ToolError("invalid_reference", str(e))
        text = _arg_str(args, "text", 4000)
        said = _arg_str(args, "said", 1000)
        kind = _arg_str(args, "kind", 20)
        status = _arg_str(args, "status", 20)
        if kind is not None and kind not in NOTE_KINDS:
            raise ToolError("invalid_argument", "kind is one of %s" % ", ".join(NOTE_KINDS))
        if status is not None and status not in NOTE_STATUS:
            raise ToolError("invalid_argument", "status is one of %s" % ", ".join(NOTE_STATUS))
        now = _now_ts()
        if action == "add":
            if not text:
                raise ToolError("invalid_argument", "text is required")
            kind = kind or "note"
            if kind == "rule" and not said:
                raise ToolError("said_required", "a rule binds every later conversation: "
                                                 "said must quote the operator who set it")
            if status in ("confirmed", "rejected") and not said:
                raise ToolError("said_required", "only the operator confirms or rejects: "
                                                 "said must quote them")
            cur = con.execute("INSERT INTO notes(inst, kind, status, text, said, by, created, "
                              "updated) VALUES(?,?,?,?,?,?,?,?)",
                              (inst, kind, status or "open", text, said, _caller(args), now, now))
            nid = cur.lastrowid
        else:
            nid = _arg_int(args, "id", 0, 1, 1 << 40)
            row = con.execute("SELECT * FROM notes WHERE id=? AND inst=?", (nid, inst)).fetchone()
            if row is None:
                raise ToolError("unknown_note", "no note %d" % nid)
            if (status in ("confirmed", "rejected") or (row["kind"] == "rule" or kind == "rule")) \
                    and not said:
                raise ToolError("said_required", "confirming, rejecting or changing a rule "
                                                 "needs said = the operator's words")
            con.execute("UPDATE notes SET kind=?, status=?, text=?, said=COALESCE(?, said), "
                        "updated=? WHERE id=?",
                        (kind or row["kind"], status or row["status"], text or row["text"],
                         said, now, nid))
            if args.get("about") is not None:
                con.execute("DELETE FROM note_refs WHERE note_id=?", (nid,))
        for k, key in refs:
            con.execute("INSERT OR IGNORE INTO note_refs(note_id, kind, key) VALUES(?,?,?)",
                        (nid, k, key))
        row = con.execute("SELECT * FROM notes WHERE id=?", (nid,)).fetchone()
        out = _note_out(con, row)
        journal(con, inst, _caller(args), "proj-note", {k: v for k, v in args.items()
                                                        if not k.startswith("_")}, {"id": nid})
    return _dumps({"ok": True, "note": out})


# -- proj-set ------------------------------------------------------------------------------

_SET_ATTRS = {
    "installation": {"name": "text", "description": "longtext", "precedence": "sources",
                     "bus_access": "longtext", "stick_id": "stick_id"},
    "area": {"name": "text", "description": "text"},
    "line": {"name": "text", "description": "text", "medium": "text"},
    "device": {"name": "text", "description": "text", "location": "text", "role": "text",
               "present": "bool", "product": "text", "order_number": "text",
               "manufacturer": "text", "app_id": "app_id"},
    "range": {"name": "text", "description": "text"},
    "group": {"name": "text", "description": "text", "dpt": "dpt", "present": "bool",
              "central": "bool", "safety": "text"},
    "object": {"name": "text", "description": "text", "dpt": "dpt"},
    "link": {"present": "bool", "send": "bool"},
    "param": {"value": "scalar"},
}


def _check_value(kind_, vtype, value):
    if value is None:
        return None
    if vtype in ("text", "longtext"):
        if not isinstance(value, str) or not value.strip():
            raise ToolError("invalid_value", "a non-empty string is expected")
        cap = 8000 if vtype == "longtext" else 300
        if len(value) > cap:
            raise ToolError("invalid_value", "longer than %d characters" % cap)
        return value.strip() if vtype == "longtext" else _clean(value)
    if vtype == "bool":
        if not isinstance(value, bool):
            raise ToolError("invalid_value", "true or false is expected")
        return value
    if vtype == "dpt":
        if not isinstance(value, str) or not re.match(r"^(DPS?T-)?\d{1,3}([.-]\d{1,3})?$",
                                                      value.strip(), re.I):
            raise ToolError("invalid_value", "a datapoint type like 1.001 or DPST-1-1")
        v = value.strip().upper().replace("DPST-", "").replace("DPT-", "").replace("-", ".")
        main, _, sub = v.partition(".")
        return "%d.%03d" % (int(main), int(sub)) if sub else str(int(main))
    if vtype == "stick_id":
        v = re.sub(r"[\s:._-]", "", str(value).lower())
        if not re.match(r"^[0-9a-f]{12}$", v):
            raise ToolError("invalid_value", "a stick id: 12 hex digits, as the stick's "
                                             "describe reports it")
        return v
    if vtype == "app_id":
        v = re.sub(r"[\s:._-]", "", str(value).upper())
        if not re.match(r"^([0-9A-F]{8}|[0-9A-F]{10})$", v):
            raise ToolError("invalid_value", "an ApplicationId: 8 or 10 hex digits")
        return v
    if vtype == "sources":
        if not isinstance(value, list) or not value or any(s not in SOURCES for s in value):
            raise ToolError("invalid_value", "a list of sources: %s" % ", ".join(DEFAULT_PRECEDENCE))
        return _complete_precedence(value)
    if vtype == "scalar":
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            raise ToolError("invalid_value", "a number or a string")
        return value
    raise ToolError("invalid_value", "unsupported")


def tool_set(store, args, budget):
    kind, key = _arg_ref(args, "ref")
    attr = _arg_str(args, "attr", 40, required=True)
    said = _arg_str(args, "said", 1000, required=True)
    if "value" not in args:
        raise ToolError("invalid_argument", "value is required (null withdraws the operator's "
                                            "earlier decision)")
    allowed = _SET_ATTRS.get(kind, {})
    if attr not in allowed:
        raise ToolError("attr_not_settable", "%s of a %s cannot be set; settable: %s"
                        % (attr, kind, ", ".join(sorted(allowed)) or "nothing"))
    value = _check_value(kind, allowed[attr], args.get("value"))
    with store.writer() as con:
        inst = _inst(con, args)
        prec_before = precedence_of(con, inst)
        before = resolve(load_claims(con, inst, kind=kind, keys=[key], attrs=[attr]), prec_before)
        now = _now_ts()
        ended = 0
        for r in con.execute("SELECT id, value FROM claims WHERE inst=? AND kind=? AND key=? AND "
                             "attr=? AND source='user' AND ended_at IS NULL",
                             (inst, kind, key, attr)).fetchall():
            if value is None or json.loads(r["value"]) != value:
                con.execute("UPDATE claims SET ended_at=? WHERE id=?", (now, r["id"]))
                ended += 1
        if value is not None:
            put_claims(con, inst, "user", [(kind, key, attr, value, "said: " + said)], now,
                       by=_caller(args))
        if kind in ("group", "device", "link") and value is not None and attr != "present":
            pres = con.execute("SELECT 1 FROM claims WHERE inst=? AND kind=? AND key=? AND "
                               "attr='present' AND ended_at IS NULL", (inst, kind, key)).fetchone()
            if pres is None:
                put_claims(con, inst, "user", [(kind, key, "present", True, "said: " + said)],
                           now, by=_caller(args))
        after = resolve(load_claims(con, inst, kind=kind, keys=[key], attrs=[attr]),
                        precedence_of(con, inst))
        result = {"ref": ref_text(kind, key), "attr": attr,
                  "before": before.get(attr, {}).get("value"),
                  "now": after.get(attr, {}).get("value"),
                  "now_from": after.get(attr, {}).get("source"),
                  "withdrawn": ended}
        journal(con, inst, _caller(args), "proj-set",
                {"ref": ref_text(kind, key), "attr": attr, "value": value, "said": said}, result)
    if kind in ("link", "param") or (kind in ("device", "group") and attr == "present"):
        result["pending"] = ("recorded as the target state; the devices still hold the old "
                             "one until they are programmed. proj-diff a=user b=bus lists it.")
    return _dumps(dict(result, ok=True))


# -- proj-read-device -------------------------------------------------------------------
#
# The one tool that reaches a bus.  It reads a device through the installation's
# busware KNX stick (that stick's MCP server on the LAN, found by mDNS) and records
# what the device holds as source "bus": identity, load state and the two
# commissioning tables.  It only reads.  A read takes longer than a client should
# wait (and longer than a stick that relays this server lets a call take), so it
# runs in the background: one call starts it, the next one fetches the result.

STICK_TIMEOUT = 40          # seconds for one stick call; a table read can take a while
MGMT_GAP_S = 0.4            # quiet time between two device dialogues, see read_device
PROP_RETRY_S = 1.0          # before reading a property again whose length was wrong
# bytes a property holds: a value of another length is not that property
_PROP_LEN = {"application_id": (4, 5), "serial_number": (6,), "order_info": (10,)}
READ_KEEP_S = 1800          # a finished read stays available this long
_READS = {}                 # (database, installation, address) -> job
_READS_LOCK = threading.Lock()
_RESOLVE_IFACE = None       # local IPv4 for mDNS questions; set by serve()


class StickError(Exception):
    pass


def mdns_resolve(host, iface=None, timeout=2.0):
    """IPv4 address of <host>.local or None.  A one-shot (legacy unicast) question,
    RFC 6762 6.7: the responder answers to our own port."""
    labels = tuple(host.split("."))
    if labels[-1].lower() != "local":
        labels += ("local",)
    query = build_message(random.randrange(1, 0xFFFF), 0, questions=[(labels, T_A, C_IN)])
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except OSError:
        return None
    try:
        s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 255)
        if iface:
            s.bind((iface, 0))
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(iface))
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            s.sendto(query, (MDNS_GROUP, MDNS_PORT))
            until = min(deadline, time.monotonic() + 0.5)
            while time.monotonic() < until:
                s.settimeout(max(0.01, until - time.monotonic()))
                try:
                    data, _src = s.recvfrom(9000)
                    msg = parse_message(data)
                except socket.timeout:
                    break
                except ValueError:
                    continue
                for r in msg["records"]:
                    if (r["type"] == T_A and r["rdlen"] == 4
                            and _key(r["name"]) == _key(labels)):
                        return socket.inet_ntoa(data[r["rdata_pos"]:r["rdata_pos"] + 4])
    except OSError:
        return None
    finally:
        s.close()
    return None


def stick_url(stick_id):
    """MCP endpoint of the stick: KNXPROJECT_STICK_URL, else busware-knx-<id>.local."""
    url = _env("KNXPROJECT_STICK_URL")
    if url:
        return url
    ip = mdns_resolve("busware-knx-%s" % stick_id, _RESOLVE_IFACE)
    return "http://%s/mcp" % ip if ip else None


def stick_call(url, tool, args, timeout=STICK_TIMEOUT):
    """One tools/call on the stick -> the verb's reply (a dict carrying ok)."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                       "params": {"name": tool, "arguments": args}}).encode("utf-8")
    req = urllib.request.Request(url, body, {"Content-Type": "application/json",
                                             "Accept": "application/json"})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout) as r:
            msg = json.loads(r.read().decode("utf-8"))
    except (OSError, ValueError) as e:
        raise StickError("%s at %s: %s" % (tool, url, e))
    if not isinstance(msg, dict) or "error" in msg:
        raise StickError("%s: %s" % (tool, (msg or {}).get("error", {}).get("message")
                                     if isinstance(msg, dict) else "no JSON-RPC reply"))
    try:
        reply = json.loads(msg["result"]["content"][0]["text"])
    except (KeyError, IndexError, TypeError, ValueError):
        raise StickError("%s: the stick sent an unexpected reply" % tool)
    if not isinstance(reply, dict):
        raise StickError("%s: the stick sent an unexpected reply" % tool)
    if msg["result"].get("isError"):
        reply.setdefault("ok", False)
    return reply


def stick_check(stick_id):
    """-> (url, facts) of the installation's stick.  Talks to the stick, not the bus."""
    if not stick_id:
        raise StickError("this record names no stick: proj-set installation stick_id "
                         "(the stick_id its describe reports) first")
    url = stick_url(stick_id)
    if not url:
        raise StickError("stick %s is not on the network (no mDNS answer for "
                         "busware-knx-%s.local)" % (stick_id, stick_id))
    d = stick_call(url, "describe", {}, timeout=10)
    got = str(d.get("stick_id") or "").lower()
    if got != stick_id:
        raise StickError("the stick at %s is %s, not this installation's stick %s"
                         % (url, got or "unknown", stick_id))
    knx = d.get("knx") if isinstance(d.get("knx"), dict) else {}
    return url, {"stick_id": got, "url": url, "own_address": knx.get("own_address"),
                 "bus_ready": knx.get("tpuart_ready")}


def read_device(store, inst, ia, stick_id, by):
    """Read one device through the stick and record it as source bus -> result."""
    t0 = time.monotonic()
    url, stick = stick_check(stick_id)
    if stick.get("bus_ready") is False:
        raise StickError("the stick reports no KNX bus (transceiver not ready)")
    # Every verb below is a dialogue of its own (connect ... disconnect), and every
    # dialogue numbers its frames from 0 again.  The stick does not acknowledge on
    # layer 2, so the device repeats each answer up to three times; a repeat that
    # arrives after the next dialogue opened carries the number that dialogue
    # waits for, and a stick firmware that does not compare the answer with the
    # question takes it.  Measured 30.10.2026: serial number and order info both
    # came back as the ApplicationId read just before.  Hence a quiet gap before
    # each dialogue, and a length check on every property.
    def mgmt(tool, args):
        time.sleep(MGMT_GAP_S)
        return stick_call(url, tool, args)

    def prop(key, obj, pid, count=None):
        args = {"device": ia, "object": obj, "pid": pid}
        if count:
            args["count"] = count
        for _attempt in range(2):
            p = mgmt("knx-prop-read", args)
            v = str(p.get("data") or "") if p.get("ok") else ""
            if not v or len(v) // 2 in _PROP_LEN[key]:
                return v
            time.sleep(PROP_RETRY_S)
        unreadable.append(key)
        return ""

    unreadable = []
    dev = {"address": ia}
    d = mgmt("knx-describe", {"device": ia})
    if not d.get("ok"):
        raise StickError("%s did not answer: %s" % (ia, d.get("error") or "no reply"))
    for key in ("mask", "lsm"):
        if d.get(key) is not None:
            dev[key] = d[key]
    v = prop("application_id", 3, 13)
    if v:
        dev["application_id"] = v
    for pid, key in ((11, "serial_number"), (15, "order_info")):
        v = prop(key, 0, pid, 1)
        if v and set(v.upper()) != {"F"}:        # all FF = never written
            dev[key] = v
    t = mgmt("knx-tables-read", {"device": ia})
    tables_ok = bool(t.get("ok"))
    if tables_ok:
        dev["associations"] = t.get("associations") or []
    ref = "read-device:%s via %s" % (ia, stick_id)
    claims = inventory_device_claims(ia, dev, ref)
    read_attrs = {a for kind, _key_, a, _v, _r in claims if kind == "device"}
    with store.reader() as con:
        known = load_claims(con, inst, kind="device", keys=[ia], attrs=["serial"], source="bus")
        links_before = {r["key"] for r in load_claims(con, inst, kind="link", keys=ia + "#*",
                                                      source="bus")}
    identity = {"mask": dev.get("mask"), "app_id": dev.get("application_id"),
                "serial": dev.get("serial_number"), "order_info": dev.get("order_info")}
    serial_known = known[0]["value"] if known else None
    if (serial_known and dev.get("serial_number")
            and str(serial_known).upper() != dev["serial_number"].upper()):
        return {"ok": False, "error": "different_device",
                "message": "%s answers with serial %s, the record has %s for this address: a "
                           "different device. Nothing was recorded." % (
                               ia, dev["serial_number"], serial_known),
                "identity": identity, "stick": stick}

    def scope(kind, key, attr):
        # only what this read restated: a property that failed to read ends nothing,
        # and the links only when the tables were read
        if kind == "device":
            return key == ia and attr in read_attrs
        if kind == "link":
            return tables_ok and key.split("#", 1)[0] == ia
        return False

    st = record_import(store, inst, "bus", "read-device", ref, _now_ts(), claims, scope,
                       by, "data")
    # what the record holds now, not what the read said: a read the record
    # already holds newer bus data for ends nothing (put_claims)
    with store.reader() as con:
        links_now = {r["key"] for r in load_claims(con, inst, kind="link", keys=ia + "#*",
                                                   source="bus")}
    out = {"ok": True, "stick": stick, "identity": identity,
           "load_state": next((v for kind, _k, a, v, _r in claims if a == "load_state"), None),
           "links_new": sorted(links_now - links_before, key=_link_sort),
           "links_gone": sorted(links_before - links_now, key=_link_sort),
           "links": len(links_now),
           "recorded": {k: st.get(k) for k in ("asserted", "new", "ended", "reopened")},
           "took_s": round(time.monotonic() - t0, 1)}
    if st.get("record_newer"):
        out["record_newer"] = st["record_newer"]
        out["note"] = ("the record holds bus data of this device dated %s, later than this "
                       "read: nothing was ended, nothing new made current" % st["record_newer"])
    if not tables_ok:
        out["tables_error"] = t.get("error") or "unreadable"
    if unreadable:
        out["unreadable"] = sorted(set(unreadable))
    return out


def _link_sort(key):
    dev, _, rest = key.partition("#")
    num, _, ga = rest.partition(">")
    return (ia_sort(dev), int(num) if num.isdigit() else 0, ga)


def _run_read(store, inst, ia, stick_id, by, job):
    try:
        res = read_device(store, inst, ia, stick_id, by)
        state = "done" if res.get("ok") else "failed"
    except StickError as e:
        res, state = {"ok": False, "error": "read_failed", "message": str(e)}, "failed"
    except Exception as e:  # noqa: BLE001 -- the job must end, whatever happened
        traceback.print_exc()
        res, state = {"ok": False, "error": "internal", "message": str(e)}, "failed"
    try:                         # journaled before it shows as finished
        with store.writer() as con:
            journal(con, inst, by, "proj-read-device", {"address": ia, "result": True},
                    dict({"state": state}, **{k: res[k] for k in (
                        "error", "message", "links", "links_new", "links_gone",
                        "record_newer") if k in res}))
    except (StoreError, sqlite3.Error):
        traceback.print_exc()
    with _READS_LOCK:
        job.update(state=state, result=res, finished=_now_ts())


def _job_out(job):
    out = {k: job[k] for k in ("address", "state", "started", "said", "by") if k in job}
    if job["state"] == "running":
        out["next"] = "still reading; call proj-read-device with the address only, in ~10 s"
    else:
        out["finished"] = job.get("finished")
        out.update(job.get("result") or {})
    return out


def tool_read_device(store, args, budget):
    ia = norm_ia(_arg_str(args, "address", 20, required=True))
    if ia is None:
        raise ToolError("invalid_reference", "address is a device address like 1.2.3")
    said = _arg_str(args, "said", 1000)
    with store.reader() as con:
        inst = _inst(con, args)
        res = resolve(load_claims(con, inst, kind="installation", keys=["-"]),
                      precedence_of(con, inst))
    stick_id = value_of(res, "stick_id")
    key = (store.path, inst, ia)
    now = time.monotonic()
    with _READS_LOCK:
        for k_, j in list(_READS.items()):
            if j["state"] != "running" and now - j["t"] > READ_KEEP_S:
                del _READS[k_]
        job = _READS.get(key)
        if said is None:
            if job:
                return _dumps(_job_out(job))
            return _dumps({"address": ia, "state": None, "stick_id": stick_id,
                           "next": "nothing read yet. To read the device, call again with "
                                   "said = the operator's words asking for it."})
        if job and job["state"] == "running":
            raise ToolError("read_running", "%s is being read; call again without said "
                                            "for the result" % ia)
        if not stick_id:
            raise ToolError("stick_unknown", "this record names no stick: proj-set "
                                             "installation stick_id first")
        job = {"address": ia, "state": "running", "said": said, "by": _caller(args),
               "started": _now_ts(), "t": now}
        _READS[key] = job
    try:
        with store.writer() as con:
            journal(con, inst, _caller(args), "proj-read-device",
                    {"address": ia, "said": said, "stick_id": stick_id}, {"started": True})
    except BaseException:
        with _READS_LOCK:
            if _READS.get(key) is job:
                del _READS[key]
        raise
    threading.Thread(target=_run_read, args=(store, inst, ia, stick_id, _caller(args), job),
                     name="read-%s" % ia, daemon=True).start()
    return _dumps({"address": ia, "state": "running", "stick_id": stick_id,
                   "next": "reading in the background (typically 5-30 s); call "
                           "proj-read-device with the address only for the result"})


# -- tool table -------------------------------------------------------------------------

_INST_PROP = {"type": "string", "description": "Installation id; only needed when the "
                                               "server holds more than one"}
_OFFSET_PROP = {"type": "integer", "minimum": 0, "description": "Start index (default 0)"}
_RO = {"readOnlyHint": True}
_RW = {"readOnlyHint": False, "destructiveHint": False, "openWorldHint": False}

TOOLS = [
    {"name": "proj-brief",
     "description": (
         "START HERE in every conversation. The installation: description, the binding "
         "rules of the house, counts, which sources were imported and when, open "
         "findings/todos, pending changes, precedence of sources."),
     "inputSchema": {"type": "object", "properties": {"installation": _INST_PROP}},
     "annotations": _RO},
    {"name": "proj-find",
     "description": (
         "Search the record: names, descriptions, functions, products, parameter texts, "
         "notes. Words must all match (case-insensitive); an address (1.2.3, 3/1/7) "
         "finds that entry. Paginated."),
     "inputSchema": {"type": "object", "properties": {
         "query": {"type": "string"},
         "kinds": {"type": "array", "items": {"type": "string", "enum": list(KINDS) + ["note"]}},
         "offset": _OFFSET_PROP, "installation": _INST_PROP},
         "required": ["query"]},
     "annotations": _RO},
    {"name": "proj-group",
     "description": (
         "Group addresses. One address (3/1/7): name with its source, every source's "
         "claim (ets, esf, fhem, ha, user...) with dates, datapoint/size, Home Assistant "
         "entities, traffic seen, the devices and com objects linked (sources per link: "
         "bus = read from the device), notes. A range (3, 3/1, *) lists addresses with "
         "name, devices and sources. Paginated."),
     "inputSchema": {"type": "object", "properties": {
         "ga": {"type": "string", "description": "3/1/7, or a range: 3, 3/1, *"},
         "offset": _OFFSET_PROP, "installation": _INST_PROP},
         "required": ["ga"]},
     "annotations": _RO},
    {"name": "proj-device",
     "description": (
         "One device by individual address. section=summary (default): product, "
         "application id, serial, which sources know it, linked com objects with their "
         "group addresses, notes. section=objects: every known com object (name, function, "
         "size, flags, groups). section=params: parameter values with their meaning, "
         "query filters (e.g. 'Treppenlicht 1.A'). Paginated."),
     "inputSchema": {"type": "object", "properties": {
         "address": {"type": "string", "description": "e.g. 1.2.3"},
         "section": {"type": "string", "enum": ["summary", "objects", "params"]},
         "query": {"type": "string"},
         "offset": _OFFSET_PROP, "installation": _INST_PROP},
         "required": ["address"]},
     "annotations": _RO},
    {"name": "proj-topology",
     "description": (
         "The installation as a structure to draw. view=physical: areas, lines, devices "
         "(with the sources that know each device); view=functional: main and middle "
         "groups, group addresses and the devices linked to them (range=3/1 narrows it). "
         "format=mermaid returns a ready Mermaid flowchart."),
     "inputSchema": {"type": "object", "properties": {
         "view": {"type": "string", "enum": ["physical", "functional"]},
         "format": {"type": "string", "enum": ["json", "mermaid"]},
         "range": {"type": "string", "description": "functional view only: 3 or 3/1"},
         "installation": _INST_PROP}},
     "annotations": _RO},
    {"name": "proj-diff",
     "description": (
         "Compare two sources: devices, application ids, links. Default a=ets b=bus "
         "(the project file against what the devices hold). a=user b=bus: what the "
         "operator decided that the devices do not hold yet."),
     "inputSchema": {"type": "object", "properties": {
         "a": {"type": "string", "enum": DEFAULT_PRECEDENCE},
         "b": {"type": "string", "enum": DEFAULT_PRECEDENCE},
         "installation": _INST_PROP}},
     "annotations": _RO},
    {"name": "proj-history",
     "description": (
         "History of one entry (ref = 1.2.3, 3/1/7, 1.2.3#24, ...): every claim ever "
         "made, with source, dates and when it ended, plus notes and journal entries. "
         "Without ref: the last changes to the record."),
     "inputSchema": {"type": "object", "properties": {
         "ref": {"type": "string"}, "offset": _OFFSET_PROP, "installation": _INST_PROP}},
     "annotations": _RO},
    {"name": "proj-note",
     "description": (
         "Write a note into the record (never the bus). kind: note, finding (your "
         "inference -- stays open until the operator or a bus read confirms it), rule "
         "(binding for every later conversation; needs said), todo. action=update changes "
         "text/status/about of note id; status confirmed/rejected needs said (the operator's "
         "words). action=list filters by kind/status. about: references (1.2.3, 3/1/7, "
         "1.2.3#24, installation). Notes shown with an entry are cut (\"cut\": true); "
         "action=list id=N gives one in full."),
     "inputSchema": {"type": "object", "properties": {
         "action": {"type": "string", "enum": ["add", "update", "list"]},
         "id": {"type": "integer"},
         "kind": {"type": "string", "enum": list(NOTE_KINDS)},
         "status": {"type": "string", "enum": list(NOTE_STATUS)},
         "text": {"type": "string"},
         "about": {"type": "array", "items": {"type": "string"}},
         "said": {"type": "string", "description": "The operator's words this rests on"},
         "offset": _OFFSET_PROP,
         "installation": _INST_PROP}},
     "annotations": _RW},
    {"name": "proj-read-device",
     "description": (
         "Read one device over the KNX bus through the installation's stick (stick_id in "
         "proj-brief) and record what it holds as source 'bus': identity, load state, "
         "address and association tables. Reads only, never writes a device -- but it puts "
         "telegrams on a working bus, so only when the operator asked: said quotes them. "
         "Runs in the background: the call with said starts it, a call with the address "
         "only returns the result (new and vanished links included). A device that answers "
         "with another serial number than the record holds is reported, not recorded."),
     "inputSchema": {"type": "object", "properties": {
         "address": {"type": "string", "description": "Device address, e.g. 1.2.3"},
         "said": {"type": "string", "description": "The operator's words asking for the "
                                                   "read; omit to fetch the result"},
         "installation": _INST_PROP},
         "required": ["address"]},
     "annotations": {"readOnlyHint": False, "destructiveHint": False,
                     "idempotentHint": False, "openWorldHint": True}},
    {"name": "proj-set",
     "description": (
         "Record a decision of the operator as a claim of source 'user' (it outranks the "
         "imported sources). Only what the operator said or asked for -- said quotes them. "
         "Names, descriptions, locations, datapoint types, the installation description, "
         "bus_access (which stick or interface reaches its bus), stick_id (the stick "
         "proj-read-device uses) and precedence; and the target state of the installation (device/group present, link "
         "present/send, parameter value), which proj-diff a=user b=bus then lists as work "
         "left to do. value null withdraws the operator's earlier decision. Changes the "
         "record only, never the bus."),
     "inputSchema": {"type": "object", "properties": {
         "ref": {"type": "string", "description": "1.2.3, 3/1/7, 3/1, 1.2.3#24, "
                                                  "1.2.3#24>3/1/7, 1.2.3:P61, installation"},
         "attr": {"type": "string"},
         "value": {"description": "string, number, boolean, list (precedence) or null"},
         "said": {"type": "string", "description": "The operator's words this rests on"},
         "installation": _INST_PROP},
         "required": ["ref", "attr", "value", "said"]},
     "annotations": _RW},
]

TOOL_FUNCS = {
    "proj-brief": tool_brief,
    "proj-find": tool_find,
    "proj-group": tool_group,
    "proj-device": tool_device,
    "proj-topology": tool_topology,
    "proj-diff": tool_diff,
    "proj-history": tool_history,
    "proj-note": tool_note,
    "proj-set": tool_set,
    "proj-read-device": tool_read_device,
}


# ---------------------------------------------------------------------------
# MCP (JSON-RPC 2.0, stateless Streamable HTTP, JSON responses only)
# ---------------------------------------------------------------------------

def read_skill():
    try:
        with open(SKILL_FILE, "rb") as f:
            return f.read().decode("utf-8")
    except (OSError, UnicodeDecodeError):
        return None


def skill_resource():
    text = read_skill()
    if text is None:
        return None
    return {"uri": SKILL_URI, "name": "SKILL.md", "title": "knxproject-mcp skill",
            "description": SKILL_DESCRIPTION, "mimeType": "text/markdown",
            "size": len(text.encode("utf-8"))}


def _rpc_error(mid, code, message):
    return {"jsonrpc": "2.0", "id": mid, "error": {"code": code, "message": message}}


def _call_budget(mid):
    empty = {"jsonrpc": "2.0", "id": mid,
             "result": {"content": [{"type": "text", "text": ""}], "isError": False}}
    # the text is JSON inside a JSON string: quotes and backslashes double
    return int((RESPONSE_BUDGET - len(_dumps(empty).encode("utf-8"))) * 0.8)


def handle_rpc(msg, store, notes=None, caller=None):
    if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0":
        return _rpc_error(msg.get("id") if isinstance(msg, dict) else None, -32600,
                          "Invalid Request")
    method = msg.get("method")
    if method is None:
        return None
    mid = msg.get("id")
    if not isinstance(method, str):
        return _rpc_error(mid, -32600, "Invalid Request")
    if notes is not None:
        notes.append(method)
    if "id" not in msg:
        return None
    params = msg.get("params") or {}
    if not isinstance(params, dict):
        return _rpc_error(mid, -32602, "params must be an object")
    if method == "initialize":
        pv = params.get("protocolVersion")
        result = {"protocolVersion": pv if pv in SUPPORTED_PROTOCOLS else DEFAULT_PROTOCOL,
                  "capabilities": {"tools": {"listChanged": False},
                                   "resources": {"subscribe": False, "listChanged": False}},
                  "serverInfo": {"name": SERVER_NAME, "version": VERSION},
                  "instructions": INSTRUCTIONS}
        return {"jsonrpc": "2.0", "id": mid, "result": result}
    if method == "ping":
        return {"jsonrpc": "2.0", "id": mid, "result": {}}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": mid, "result": {"tools": TOOLS}}
    if method == "resources/list":
        res = skill_resource()
        return {"jsonrpc": "2.0", "id": mid, "result": {"resources": [res] if res else []}}
    if method == "resources/templates/list":
        return {"jsonrpc": "2.0", "id": mid, "result": {"resourceTemplates": []}}
    if method == "resources/read":
        uri = params.get("uri")
        text = read_skill() if uri == SKILL_URI else None
        if text is None:
            return {"jsonrpc": "2.0", "id": mid, "error": {
                "code": -32002, "message": "Resource not found", "data": {"uri": uri}}}
        return {"jsonrpc": "2.0", "id": mid, "result": {"contents": [
            {"uri": SKILL_URI, "mimeType": "text/markdown", "text": text}]}}
    if method == "tools/call":
        name = params.get("name")
        func = TOOL_FUNCS.get(name)
        if func is None:
            return _rpc_error(mid, -32602, "Unknown tool: %s" % (name,))
        if notes is not None:
            notes.append(name)
        args = params.get("arguments")
        if args is None:
            args = {}
        is_error = False
        try:
            if not isinstance(args, dict):
                raise ToolError("invalid_argument", "arguments must be an object")
            args = {k: v for k, v in args.items() if not str(k).startswith("_")}
            if caller:
                args["_by"] = caller
            text = func(store, args, _call_budget(mid))
        except ToolError as e:
            text, is_error = _dumps(e.payload), True
        except (StoreError, sqlite3.Error) as e:
            text, is_error = _dumps({"error": "store_error", "message": str(e)[:300]}), True
        except Exception as e:                       # pragma: no cover - logged
            traceback.print_exc()
            text = _dumps({"error": "internal_error", "message": str(e)[:300]})
            is_error = True
        resp = {"jsonrpc": "2.0", "id": mid,
                "result": {"content": [{"type": "text", "text": text}], "isError": is_error}}
        if len(_dumps(resp).encode("utf-8")) > HARD_REPLY_CAP:
            resp["result"] = {"content": [{"type": "text", "text": _dumps({
                "error": "reply_too_large",
                "message": "the answer does not fit one reply; narrow the request "
                           "(offset, query, range)"})}], "isError": True}
        return resp
    return _rpc_error(mid, -32601, "Method not found: %s" % method)


# ---------------------------------------------------------------------------
# mDNS / DNS-SD responder (RFC 6762, RFC 6763), deliberately small
#
# Announces _knxproject-mcp._tcp.local so that clients on the LAN find the server
# without a URL.  It answers PTR (service and service enumeration), SRV, TXT
# and A queries for its own names, honours the QU bit and legacy unicast
# queries, probes its instance name once at start (" (2)" etc. on conflict),
# announces three times and says goodbye (TTL 0) on a clean shutdown.  It
# shares UDP port 5353 with other responders on the host.
# ---------------------------------------------------------------------------

MDNS_GROUP = "224.0.0.251"
MDNS_PORT = 5353
SERVICE_LABELS = ("_knxproject-mcp", "_tcp", "local")
ENUM_LABELS = ("_services", "_dns-sd", "_udp", "local")
T_A, T_PTR, T_TXT, T_AAAA, T_SRV, T_ANY = 1, 12, 16, 28, 33, 255
C_IN = 1
CACHE_FLUSH = 0x8000
QU_BIT = 0x8000
TTL_HOST = 120                     # SRV, A
TTL_OTHER = 4500                   # PTR, TXT
LEGACY_TTL = 10                    # RFC 6762 6.7: at most 10 s for legacy unicast
FLAGS_RESPONSE = 0x8400            # QR + AA
IFF_UP, IFF_LOOPBACK, IFF_POINTOPOINT, IFF_MULTICAST = 0x1, 0x8, 0x10, 0x1000
_BRIDGE_IF_RE = re.compile(r"^(docker\d*|br-|veth|virbr|lxcbr|lxdbr|cni|podman|flannel|cali|vxlan|kube)")
# default address pools of Docker (bridge networks) and Podman
_CONTAINER_NETS = ([ipaddress.ip_network("172.%d.0.0/16" % i) for i in range(17, 32)]
                   + [ipaddress.ip_network("10.88.0.0/16")])


def _key(labels):
    return tuple(lb.lower() for lb in labels)


def encode_name(labels):
    out = bytearray()
    for lb in labels:
        b = lb.encode("utf-8")
        if not 0 < len(b) <= 63:
            raise ValueError("DNS label must have 1..63 bytes: %r" % lb)
        out.append(len(b))
        out += b
    out.append(0)
    if len(out) > 255:
        raise ValueError("DNS name longer than 255 bytes")
    return bytes(out)


def read_name(data, pos):
    """Decode a (possibly compressed) name at `pos` -> (labels, next position).
    Pointers must point backwards, which rules out loops."""
    labels, end, jumps, total = [], None, 0, 0
    while True:
        if pos >= len(data):
            raise ValueError("name runs past the packet")
        n = data[pos]
        if n & 0xC0 == 0xC0:
            if pos + 1 >= len(data):
                raise ValueError("truncated compression pointer")
            ptr = ((n & 0x3F) << 8) | data[pos + 1]
            if ptr >= pos:
                raise ValueError("compression pointer does not point backwards")
            if end is None:
                end = pos + 2
            jumps += 1
            if jumps > 64:
                raise ValueError("too many compression pointers")
            pos = ptr
            continue
        if n & 0xC0:
            raise ValueError("unsupported label type")
        pos += 1
        if n == 0:
            break
        if pos + n > len(data):
            raise ValueError("label runs past the packet")
        total += n + 1
        if total > 255:
            raise ValueError("name longer than 255 bytes")
        labels.append(bytes(data[pos:pos + n]).decode("utf-8", "replace"))
        pos += n
    return tuple(labels), end if end is not None else pos


def parse_message(data):
    if len(data) < 12:
        raise ValueError("short DNS message")
    mid, flags, qd, an, ns, ar = struct.unpack_from("!6H", data, 0)
    pos, questions, records = 12, [], []
    for _ in range(qd):
        name, pos = read_name(data, pos)
        if pos + 4 > len(data):
            raise ValueError("truncated question")
        qtype, qclass = struct.unpack_from("!2H", data, pos)
        pos += 4
        questions.append((name, qtype, qclass))
    for section, count in (("an", an), ("ns", ns), ("ar", ar)):
        for _ in range(count):
            name, pos = read_name(data, pos)
            if pos + 10 > len(data):
                raise ValueError("truncated record")
            rtype, rclass, ttl, rdlen = struct.unpack_from("!2HIH", data, pos)
            pos += 10
            if pos + rdlen > len(data):
                raise ValueError("truncated record data")
            records.append({"section": section, "name": name, "type": rtype, "class": rclass,
                            "ttl": ttl, "rdata_pos": pos, "rdlen": rdlen})
            pos += rdlen
    return {"id": mid, "flags": flags, "questions": questions, "records": records,
            "data": bytes(data)}


def record_value(msg, r):
    """Comparable form of a record's data (names decompressed)."""
    d, p = msg["data"], r["rdata_pos"]
    if r["type"] == T_PTR:
        return _key(read_name(d, p)[0])
    if r["type"] == T_SRV:
        if r["rdlen"] < 7:
            raise ValueError("short SRV record")
        return struct.unpack_from("!3H", d, p) + (_key(read_name(d, p + 6)[0]),)
    return d[p:p + r["rdlen"]]


def build_message(mid, flags, questions=(), answers=(), authority=(), additionals=()):
    out = bytearray(struct.pack("!6H", mid, flags, len(questions), len(answers),
                                len(authority), len(additionals)))
    for labels, qtype, qclass in questions:
        out += encode_name(labels) + struct.pack("!2H", qtype, qclass)
    for blob in list(answers) + list(authority) + list(additionals):
        out += blob
    return bytes(out)


class _Rec:
    __slots__ = ("labels", "rtype", "ttl", "unique", "rdata", "value")

    def __init__(self, labels, rtype, ttl, unique, rdata, value):
        self.labels, self.rtype, self.ttl = labels, rtype, ttl
        self.unique, self.rdata, self.value = unique, rdata, value

    def wire(self, ttl=None, flush=True):
        rclass = C_IN | (CACHE_FLUSH if self.unique and flush else 0)
        return (encode_name(self.labels) + struct.pack(
            "!2HIH", self.rtype, rclass, self.ttl if ttl is None else ttl, len(self.rdata))
            + self.rdata)


def next_instance_name(name):
    """'x' -> 'x (2)', 'x (2)' -> 'x (3)'; stays within one 63-byte label."""
    m = re.match(r"^(.*) \((\d+)\)$", name)
    base, n = (m.group(1), int(m.group(2)) + 1) if m else (name, 2)
    suffix = " (%d)" % n
    return base.encode("utf-8")[:63 - len(suffix)].decode("utf-8", "ignore") + suffix


def clip_label(text):
    return text.encode("utf-8")[:63].decode("utf-8", "ignore")


class MdnsCore:
    """The records and the answering logic, without any socket."""

    def __init__(self, instance, host, port, addresses, txt):
        self.host, self.port = host, port
        self.addresses = list(addresses)
        self.txt = list(txt)
        self.set_instance(instance)

    def set_instance(self, instance):
        self.instance = clip_label(instance)
        self.instance_labels = (self.instance,) + SERVICE_LABELS
        self.host_labels = (self.host, "local")
        srv = struct.pack("!3H", 0, 0, self.port) + encode_name(self.host_labels)
        txt = b"".join(bytes([len(s.encode("utf-8"))]) + s.encode("utf-8") for s in self.txt)
        self.ptr = _Rec(SERVICE_LABELS, T_PTR, TTL_OTHER, False,
                        encode_name(self.instance_labels), _key(self.instance_labels))
        self.enum = _Rec(ENUM_LABELS, T_PTR, TTL_OTHER, False, encode_name(SERVICE_LABELS),
                         _key(SERVICE_LABELS))
        self.srv = _Rec(self.instance_labels, T_SRV, TTL_HOST, True, srv,
                        (0, 0, self.port, _key(self.host_labels)))
        self.txt_rec = _Rec(self.instance_labels, T_TXT, TTL_OTHER, True, txt, txt)
        self.a = [_Rec(self.host_labels, T_A, TTL_HOST, True, socket.inet_aton(a),
                       socket.inet_aton(a)) for a in self.addresses]

    def records(self):
        return [self.ptr, self.enum, self.srv, self.txt_rec] + self.a

    def announcement(self, ttl=None):
        return build_message(0, FLAGS_RESPONSE, answers=[r.wire(ttl) for r in self.records()])

    def probe(self):
        return build_message(0, 0, questions=[(self.instance_labels, T_ANY, C_IN)],
                             authority=[self.srv.wire(flush=False),
                                        self.txt_rec.wire(flush=False)])

    def _lookup(self, labels, qtype):
        k = _key(labels)
        answers, extra = [], []
        if k == _key(SERVICE_LABELS) and qtype in (T_PTR, T_ANY):
            answers.append(self.ptr)
            extra += [self.srv, self.txt_rec] + self.a
        elif k == _key(ENUM_LABELS) and qtype in (T_PTR, T_ANY):
            answers.append(self.enum)
        elif k == _key(self.instance_labels):
            if qtype in (T_SRV, T_ANY):
                answers.append(self.srv)
                extra += self.a
            if qtype in (T_TXT, T_ANY):
                answers.append(self.txt_rec)
        elif k == _key(self.host_labels) and qtype in (T_A, T_ANY):
            answers += self.a
        return answers, extra

    def respond(self, msg, src):
        """[(packet, destination)] for a parsed query from `src`."""
        if msg["flags"] & 0x8000 or (msg["flags"] >> 11) & 0xF:
            return []                        # responses and non-QUERY opcodes
        known = {}
        for r in msg["records"]:
            if r["section"] == "an":
                try:
                    key = (_key(r["name"]), r["type"], record_value(msg, r))
                except (ValueError, struct.error):
                    continue
                known[key] = max(known.get(key, 0), r["ttl"])
        answers, extra, all_qu = [], [], True
        for labels, qtype, qclass in msg["questions"]:
            if qclass & 0x7FFF not in (C_IN, T_ANY):
                continue
            found, more = self._lookup(labels, qtype)
            found = [rec for rec in found if known.get(
                (_key(rec.labels), rec.rtype, rec.value), -1) < rec.ttl / 2]
            if found:
                all_qu = all_qu and bool(qclass & QU_BIT)
                answers += [rec for rec in found if rec not in answers]
                extra += [rec for rec in more if rec not in extra]
        if not answers:
            return []
        extra = [rec for rec in extra if rec not in answers]
        if src[1] != MDNS_PORT:              # legacy unicast: plain DNS rules
            pkt = build_message(
                msg["id"], FLAGS_RESPONSE, questions=msg["questions"],
                answers=[r.wire(min(r.ttl, LEGACY_TTL), flush=False) for r in answers],
                additionals=[r.wire(min(r.ttl, LEGACY_TTL), flush=False) for r in extra])
            return [(pkt, src)]
        pkt = build_message(0, FLAGS_RESPONSE, answers=[r.wire() for r in answers],
                            additionals=[r.wire() for r in extra])
        return [(pkt, src if all_qu else (MDNS_GROUP, MDNS_PORT))]

    def conflicts(self, msg):
        """True if another responder claims our instance name with other data."""
        if not msg["flags"] & 0x8000:
            return False
        mine = {T_SRV: self.srv.value, T_TXT: self.txt_rec.value}
        key = _key(self.instance_labels)
        for r in msg["records"]:
            if _key(r["name"]) != key or r["ttl"] == 0:
                continue
            try:
                value = record_value(msg, r)
            except (ValueError, struct.error):
                continue
            if mine.get(r["type"]) != value:
                return True
        return False


# -- which address to announce

def ipv4_interfaces():
    """[{name, address, netmask, flags}] of this network namespace via ioctl
    (Linux); an empty list where that is not available."""
    out = []
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except OSError:
        return out
    try:
        size = 40 if struct.calcsize("P") == 8 else 32          # sizeof(struct ifreq)
        buf = array.array("B", bytes(size * 256))
        res = fcntl.ioctl(s.fileno(), 0x8912,                    # SIOCGIFCONF
                          struct.pack("iL", size * 256, buf.buffer_info()[0]))
        length = struct.unpack("iL", res)[0]
        data = buf.tobytes()[:length]
        for off in range(0, length, size):
            name = data[off:off + 16].split(b"\0", 1)[0].decode("ascii", "replace")
            ifreq = struct.pack("256s", name.encode("ascii", "replace")[:15])
            flags = struct.unpack_from("H", fcntl.ioctl(s.fileno(), 0x8913, ifreq), 16)[0]
            try:
                netmask = socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x891B, ifreq)[20:24])
            except OSError:
                netmask = None
            out.append({"name": name, "address": socket.inet_ntoa(data[off + 20:off + 24]),
                        "netmask": netmask, "flags": flags})
    except (OSError, ValueError, struct.error):
        return out
    finally:
        s.close()
    return out


def default_route_ipv4():
    """Source address the kernel would use for the default route (no packet is sent)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except OSError:
        return None
    try:
        s.connect(("192.0.2.1", 9))
        a = s.getsockname()[0]
        return None if a.startswith("0.") else a
    except OSError:
        return None
    finally:
        s.close()


def in_container():
    if os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv"):
        return True
    try:
        with open("/proc/1/cgroup", encoding="utf-8", errors="replace") as f:
            c = f.read()
    except OSError:
        return False
    return any(k in c for k in ("docker", "containerd", "kubepods", "libpod"))


BRIDGE_HINT = ("mDNS discovery skipped: only container-internal addresses are visible (%s). "
               "Discovery needs host networking (Docker: network_mode: host or --network host). "
               "Clients then need the URL, or set KNXPROJECT_MDNS_ADDRESS.")


def mdns_plan(bind, override=None, interfaces=None, default_route=None, container=None):
    """Decide which IPv4 address(es) to announce and on which interface.
    Returns {addresses, interface, netmask, reason, warning}; no addresses
    means skip mDNS (reason says why)."""
    if interfaces is None:
        interfaces = ipv4_interfaces()
    if container is None:
        container = in_container()
    # a container that sees no bridge or veth interfaces runs in a bridge network itself
    bridged_ns = container and not any(_BRIDGE_IF_RE.match(i["name"]) for i in interfaces)

    def internal(addr, ifname=None):
        if ifname and _BRIDGE_IF_RE.match(ifname):
            return True
        ip = ipaddress.ip_address(addr)
        return bridged_ns and any(ip in n for n in _CONTAINER_NETS)

    def iface(addr):
        return next((i for i in interfaces if i["address"] == addr), None)

    def skip(reason, warning=True):
        return {"addresses": [], "interface": None, "netmask": None, "reason": reason,
                "warning": warning}

    def use(addrs):
        i = iface(addrs[0])
        return {"addresses": addrs, "interface": addrs[0], "netmask": i and i["netmask"],
                "reason": None, "warning": False}

    if override:
        addrs = [a for a in override if not ipaddress.ip_address(a).is_loopback]
        return use(addrs) if addrs else skip("KNXPROJECT_MDNS_ADDRESS holds only loopback addresses")
    b = (bind or "").strip()
    if b.startswith("[") and b.endswith("]"):
        b = b[1:-1]
    ip = _ip(b) if b else None
    if ip is not None and ip.version == 6 and not ip.is_unspecified:
        return skip("listening on an IPv6 address; mDNS here announces IPv4 only")
    if ip is not None and not ip.is_unspecified:
        if ip.is_loopback:
            return skip("listening on loopback only; nothing to announce", warning=False)
        i = iface(str(ip))
        if internal(str(ip), i and i["name"]):
            if container:
                return skip(BRIDGE_HINT % ip)
            return skip("%s belongs to a container or VM bridge (%s); mDNS announces LAN "
                        "addresses only" % (ip, i and i["name"]))
        return use([str(ip)])
    usable = []
    for i in interfaces:
        a = ipaddress.ip_address(i["address"])
        if (i["flags"] & IFF_UP and i["flags"] & IFF_MULTICAST
                and not i["flags"] & (IFF_LOOPBACK | IFF_POINTOPOINT)
                and not a.is_loopback and not a.is_link_local
                and not internal(i["address"], i["name"])):
            usable.append(i)
    if not usable:
        seen = sorted({i["address"] for i in interfaces
                       if not ipaddress.ip_address(i["address"]).is_loopback})
        if container:
            return skip(BRIDGE_HINT % (", ".join(seen) or "none"))
        return skip("no multicast-capable LAN interface with an IPv4 address")
    dr = default_route_ipv4() if default_route is None else default_route
    chosen = next((i for i in usable if i["address"] == dr), usable[0])
    base = chosen["name"].split(":", 1)[0]
    addrs = [i["address"] for i in usable if i["name"].split(":", 1)[0] == base]
    addrs.sort(key=lambda a: a != chosen["address"])
    return {"addresses": addrs, "interface": chosen["address"], "netmask": chosen["netmask"],
            "reason": None, "warning": False}


def default_mdns_hostname():
    h = re.sub(r"[^A-Za-z0-9-]", "-", socket.gethostname().split(".")[0]).strip("-")
    return h[:63] or "knxproject-mcp"


class MdnsResponder(threading.Thread):
    """Owns the UDP socket: probe, announce, answer, goodbye."""

    def __init__(self, core, plan, log, sock=None):
        super().__init__(name="knxproject-mdns", daemon=True)
        self.core, self.plan, self.log, self.sock = core, plan, log, sock
        self.stop_event = threading.Event()
        self.status = "starting"
        self.announced = False
        self._lock = threading.Lock()
        self._conflict_logged = False
        self._net = None
        if plan.get("netmask"):
            self._net = ipaddress.ip_network("%s/%s" % (plan["interface"], plan["netmask"]),
                                             strict=False)

    def info(self):
        return {"enabled": self.status in ("starting", "probing", "announced"),
                "name": self.core.instance, "address": self.core.addresses[0],
                "status": self.status}

    def _open(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, "SO_REUSEPORT"):
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            s.bind(("", MDNS_PORT))
            iface = socket.inet_aton(self.plan["interface"])
            s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
                         socket.inet_aton(MDNS_GROUP) + iface)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, iface)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 255)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_TTL, 255)
        except OSError:
            s.close()
            raise
        return s

    def _send(self, pkt, dest):
        with self._lock:
            if self.sock is not None:
                self.sock.sendto(pkt, dest)

    def _recv(self, timeout):
        self.sock.settimeout(max(0.01, timeout))
        try:
            data, src = self.sock.recvfrom(9000)
        except (socket.timeout, BlockingIOError, InterruptedError):
            return None
        if self._net is not None:
            try:
                if ipaddress.ip_address(src[0]) not in self._net:
                    return None              # only on-link senders are answered
            except ValueError:
                return None
        try:
            return parse_message(data), src
        except (ValueError, struct.error):
            return None

    def probe(self):
        """Claim the instance name: three probes 250 ms apart; on a conflict
        take the next name (' (2)', ' (3)', ...)."""
        self.status = "probing"
        time.sleep(random.uniform(0, 0.25))
        for _ in range(16):
            conflict = False
            for _ in range(3):
                self._send(self.core.probe(), (MDNS_GROUP, MDNS_PORT))
                end = time.monotonic() + 0.25
                while not conflict and not self.stop_event.is_set():
                    left = end - time.monotonic()
                    if left <= 0:
                        break
                    got = self._recv(left)
                    if got and self.core.conflicts(got[0]):
                        conflict = True
                if conflict or self.stop_event.is_set():
                    break
            if not conflict:
                return self.core.instance
            old = self.core.instance
            self.core.set_instance(next_instance_name(old))
            self.log("%s mDNS: %r is taken on the network, trying %r"
                     % (_utc_now(), old, self.core.instance))
        return self.core.instance

    def run(self):
        try:
            if self.sock is None:
                self.sock = self._open()
        except OSError as e:
            self.status = "error"
            self.log("%s mDNS: cannot use UDP port %d (%s); discovery is off"
                     % (_utc_now(), MDNS_PORT, e.strerror or e))
            return
        try:
            self.probe()
            if self.stop_event.is_set():
                return
            due = [time.monotonic() + d for d in (0.0, 1.0, 2.0)]
            while not self.stop_event.is_set():
                now = time.monotonic()
                if due and due[0] <= now:
                    due.pop(0)
                    self._send(self.core.announcement(), (MDNS_GROUP, MDNS_PORT))
                    if not self.announced:
                        self.announced = True
                        self.status = "announced"
                        self.log("%s mDNS: announcing %r (%s.local -> %s, port %d)" % (
                            _utc_now(), self.core.instance, self.core.host,
                            ", ".join(self.core.addresses), self.core.port))
                    continue
                got = self._recv(min(1.0, due[0] - now) if due else 1.0)
                if not got:
                    continue
                msg, src = got
                if msg["flags"] & 0x8000:
                    if not self._conflict_logged and self.core.conflicts(msg):
                        self._conflict_logged = True
                        self.log("%s mDNS: another responder (%s) claims %r with other data"
                                 % (_utc_now(), src[0], self.core.instance))
                    continue
                for pkt, dest in self.core.respond(msg, src):
                    self._send(pkt, dest)
        except OSError as e:
            if not self.stop_event.is_set():
                self.status = "error"
                self.log("%s mDNS: stopped after a socket error (%s)"
                         % (_utc_now(), e.strerror or e))

    def stop(self):
        """Stop answering; send a goodbye (TTL 0) if we announced anything."""
        self.stop_event.set()
        if self.is_alive():
            self.join(3)
        try:
            if self.announced:
                self._send(self.core.announcement(ttl=0), (MDNS_GROUP, MDNS_PORT))
        except OSError:
            pass
        with self._lock:
            if self.sock is not None:
                self.sock.close()
                self.sock = None
        if self.status != "error":
            self.status = "stopped"


# ---------------------------------------------------------------------------
# HTTP: Host and Origin guards
#
# Host header (DNS rebinding guard): IP literals, localhost and *.local names
# are accepted, plus the names an operator configures; every other name is
# refused.  A browser Origin on /mcp must pass the same test.
# ---------------------------------------------------------------------------

class ConfigError(Exception):
    pass


_LABEL_RE = re.compile(r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$")


def split_host_port(value):
    """'name', 'name:port', '1.2.3.4:port', '[v6]', '[v6]:port' or a bare IPv6
    literal -> (host, port or None); (None, None) when malformed."""
    v = (value or "").strip()
    if not v:
        return None, None
    if v.startswith("["):
        end = v.find("]")
        if end < 0:
            return None, None
        host, rest = v[1:end], v[end + 1:]
        if rest and not re.match(r"^:\d{1,5}$", rest):
            return None, None
        return host, int(rest[1:]) if rest else None
    if v.count(":") == 1:
        host, port = v.split(":")
        if not re.match(r"^\d{1,5}$", port):
            return None, None
        return host, int(port)
    return v, None


def _ip(host):
    try:
        return ipaddress.ip_address(host.split("%", 1)[0])
    except ValueError:
        return None


def _dns_name(host):
    """Lower-case DNS name without trailing dot, or None if not a valid name."""
    name = host.lower()
    if name.endswith("."):
        name = name[:-1]
    if not name or len(name) > 253 or not all(_LABEL_RE.match(lb) for lb in name.split(".")):
        return None
    return name


def normalize_host_entry(entry):
    """Configured extra host name -> canonical form (port and case dropped)."""
    host, _port = split_host_port(entry)
    name = _dns_name(host) if host else None
    if name is None and (host is None or _ip(host) is None):
        raise ConfigError("not a host name: %r" % entry)
    return name or str(_ip(host))


def host_allowed(value, extra=()):
    host, port = split_host_port(value)
    if host is None or (port is not None and not 0 < port < 65536):
        return False
    if _ip(host) is not None:
        return True
    name = _dns_name(host)
    if name is None:
        return False
    return name == "localhost" or name.endswith(".local") or name in extra


def parse_origin(origin):
    """'http(s)://host[:port]' -> (scheme, host, port or None); None otherwise.
    Default ports are dropped so that equal origins compare equal."""
    o = (origin or "").strip()
    m = re.match(r"^(https?)://([^/?#@]+)/?$", o, re.I)
    if not m:
        return None
    scheme = m.group(1).lower()
    host, port = split_host_port(m.group(2))
    if host is None or (port is not None and not 0 < port < 65536):
        return None
    ip = _ip(host)
    if ip is not None:
        host = str(ip)
    else:
        host = _dns_name(host)
        if host is None:
            return None
    if port == {"http": 80, "https": 443}[scheme]:
        port = None
    return scheme, host, port


def mcp_origin_allowed(origin, extra=()):
    o = parse_origin(origin)
    if o is None:
        return False
    host = o[1]
    return host_allowed("[%s]" % host if ":" in host else host, extra)


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class _HttpError(Exception):
    def __init__(self, status, error, close=False):
        super().__init__(error)
        self.status, self.error, self.close = status, error, close


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "knxproject-mcp/" + VERSION
    sys_version = ""
    timeout = 120
    disable_nagle_algorithm = True       # small replies to a stick: see knxprod-mcp 0.6

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def do_OPTIONS(self):
        self._dispatch("OPTIONS")

    def do_DELETE(self):
        self._dispatch("DELETE")

    def do_PUT(self):
        self._dispatch("PUT")

    def _send(self, status, body=b"", ctype="application/json; charset=utf-8", extra=None,
              close=False):
        self.send_response(status)
        if body or status not in (202, 204):
            self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        if close:
            self.send_header("Connection", "close")
            self.close_connection = True
        self.end_headers()
        if body:
            self.wfile.write(body)
        self._status, self._nbytes = status, len(body)

    def _json(self, status, obj, extra=None, close=False):
        self._send(status, _dumps(obj).encode("utf-8"), extra=extra, close=close)

    def _log(self, method):
        dt = (time.monotonic() - self._t0) * 1000.0
        line = "%s %s %s %s %s %dB %.1fms" % (_utc_now(), self.client_address[0], method,
                                              self.path[:160], self._status, self._nbytes, dt)
        if self._notes:
            line += " " + " ".join(self._notes)[:200]
        self.server.log_line(line)

    def _read_body(self, cap):
        if "chunked" in (self.headers.get("Transfer-Encoding") or "").lower():
            buf = bytearray()
            while True:
                line = self.rfile.readline(1024)
                try:
                    n = int(line.split(b";")[0].strip(), 16)
                except ValueError:
                    raise _HttpError(400, "bad_chunked_encoding", close=True)
                if n == 0:
                    while self.rfile.readline(1024) not in (b"\r\n", b"\n", b""):
                        pass
                    return bytes(buf)
                if len(buf) + n > cap:
                    raise _HttpError(413, "body_too_large", close=True)
                buf += self.rfile.read(n)
                self.rfile.readline(16)
        cl = self.headers.get("Content-Length")
        if cl is None:
            raise _HttpError(411, "length_required", close=True)
        try:
            n = int(cl)
        except ValueError:
            raise _HttpError(400, "bad_content_length", close=True)
        if n < 0 or n > cap:
            raise _HttpError(413, "body_too_large", close=True)
        body = self.rfile.read(n)
        if len(body) != n:
            raise _HttpError(400, "short_body", close=True)
        return body

    def _dispatch(self, method):
        self._t0 = time.monotonic()
        self._status, self._nbytes, self._notes = None, 0, []
        try:
            path = urlsplit(self.path).path
            if not host_allowed(self.headers.get("Host"), self.server.extra_hosts):
                self._notes.append("host=%r" % (self.headers.get("Host"),))
                self._json(403, {"error": "forbidden_host",
                                 "message": "Host header not allowed: use an IP address, "
                                            "localhost, a .local name or a configured name"},
                           close=True)
            elif path == "/mcp":
                if method == "POST":
                    self._mcp()
                else:
                    self._json(405, {"error": "method_not_allowed"}, extra={"Allow": "POST"})
            elif path == "/healthz" and method == "GET":
                st = self.server.store.stats()
                self._json(200, {"ok": True, "version": VERSION,
                                 "installations": st["installations"], "claims": st["claims"],
                                 "skill": os.path.isfile(SKILL_FILE),
                                 "mdns": self.server.mdns_info()})
            else:
                self._json(404, {"error": "not_found"})
        except _HttpError as e:
            try:
                self._json(e.status, {"error": e.error}, close=e.close)
            except OSError:
                self._status = self._status or e.status
                self.close_connection = True
        except (BrokenPipeError, ConnectionResetError):
            self._status = self._status or 499
            self.close_connection = True
        except Exception as e:                      # pragma: no cover - logged
            traceback.print_exc()
            try:
                self._json(500, {"error": "internal_error", "message": str(e)[:200]},
                           close=True)
            except OSError:
                pass
        finally:
            self._log(method)

    def _mcp(self):
        origin = self.headers.get("Origin")
        if origin is not None and not mcp_origin_allowed(origin, self.server.extra_hosts):
            raise _HttpError(403, "forbidden_origin")
        body = self._read_body(MAX_MCP_BODY)
        try:
            msg = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            self._json(400, _rpc_error(None, -32700, "Parse error"))
            return
        store = self.server.store
        caller = "mcp@%s" % self.client_address[0]
        if isinstance(msg, list):
            if not msg:
                self._json(400, _rpc_error(None, -32600, "Invalid Request"))
                return
            out = [r for r in (handle_rpc(m, store, self._notes, caller) for m in msg)
                   if r is not None]
            if out:
                self._json(200, out)
            else:
                self._send(202)
            return
        resp = handle_rpc(msg, store, self._notes, caller)
        if resp is None:
            self._send(202)
        else:
            self._json(200, resp)


def _print_line(line):
    print(line, flush=True)


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, handler, family):
        self.address_family = family
        super().__init__(address, handler, bind_and_activate=False)

    def server_bind(self):
        if self.address_family == socket.AF_INET6:
            try:
                self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            except (OSError, AttributeError):
                pass
        socketserver.TCPServer.server_bind(self)
        self.server_name = str(self.server_address[0])
        self.server_port = self.server_address[1]


def is_any_address(bind):
    b = (bind or "").strip()
    if b.startswith("[") and b.endswith("]"):
        b = b[1:-1]
    if not b:
        return True
    ip = _ip(b)
    return ip is not None and ip.is_unspecified


MDNS_OFF = {"enabled": False, "name": None, "address": None, "status": "off"}


def make_server(bind, port, data_dir, allowed_hosts=(), allow_any_bind=False, activate=True):
    b = (bind or "").strip()
    if b.startswith("[") and b.endswith("]"):
        b = b[1:-1]
    if b.lower() == "auto":
        plan = mdns_plan("0.0.0.0")
        if not plan["addresses"]:
            raise ConfigError("--bind auto found no LAN address: %s" % plan["reason"])
        b = plan["interface"]
    if is_any_address(b) and not allow_any_bind:
        raise ConfigError("refusing to listen on all interfaces (%r) without --allow-any-bind "
                          "(KNXPROJECT_ALLOW_ANY_BIND=1); give an address of this host instead"
                          % (bind,))
    family = socket.AF_INET6 if ":" in b else socket.AF_INET
    httpd = _Server((b, port), Handler, family)
    try:
        httpd.server_bind()
        if activate:
            httpd.server_activate()
    except OSError as e:
        httpd.server_close()
        raise ConfigError("cannot listen on %s port %d: %s" % (b or "*", port, e.strerror or e))
    httpd.store = Store(data_dir)
    httpd.extra_hosts = frozenset(normalize_host_entry(h) for h in allowed_hosts)
    httpd.log_line = _print_line
    httpd.mdns_info = lambda: dict(MDNS_OFF)
    return httpd


class Service:
    def __init__(self, cfg, log=None):
        self.cfg = cfg
        self.log = log or _print_line
        self.data = os.path.abspath(cfg["data"])
        self.httpd = make_server(cfg["bind"], cfg["port"], self.data,
                                 cfg.get("allowed_hosts", ()), cfg.get("allow_any_bind", False),
                                 activate=False)
        self.httpd.log_line = self.log
        self.port = self.httpd.server_address[1]
        self.mdns = None
        if read_skill() is None:
            self.log("%s warning: %s is missing, so the skill resource %s is not served"
                     % (_utc_now(), SKILL_FILE, SKILL_URI))

    def activate(self):
        self.httpd.server_activate()

    def start_mdns(self):
        m = self.cfg.get("mdns") or {}
        if not m.get("enabled"):
            return None
        plan = mdns_plan(self.httpd.server_address[0], m.get("address") or None)
        if not plan["addresses"]:
            self.log("%s %s" % (_utc_now(), plan["reason"]))
            info = dict(MDNS_OFF, status="skipped", reason=plan["reason"])
            self.httpd.mdns_info = lambda: dict(info)
            return None
        hostname = m.get("hostname") or default_mdns_hostname()
        core = MdnsCore(m.get("name") or "knxproject-mcp on %s" % hostname, hostname, self.port,
                        plan["addresses"], ["txtvers=1", "path=/mcp", "v=%s" % VERSION])
        self.mdns = MdnsResponder(core, plan, self.log)
        self.httpd.mdns_info = self.mdns.info
        self.mdns.start()
        return self.mdns

    def close(self):
        if self.mdns:
            self.mdns.stop()
        self.httpd.server_close()


def serve(cfg):
    svc = Service(cfg)
    svc.activate()
    info_path = os.path.join(svc.data, "server.json")
    atomic_write_json(info_path, {"host": svc.httpd.server_address[0], "port": svc.port,
                                  "pid": os.getpid(), "version": VERSION, "started": _utc_now()})
    svc.start_mdns()
    global _RESOLVE_IFACE
    bound = _ip(svc.httpd.server_address[0])
    if bound is not None and bound.version == 4 and not bound.is_unspecified \
            and not bound.is_loopback:
        _RESOLVE_IFACE = str(bound)

    def _stop(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _stop)
    host = svc.httpd.server_address[0]
    svc.log("%s knxproject-mcp %s listening on http://%s:%d/mcp%s" % (
        _utc_now(), VERSION, "[%s]" % host if ":" in host else host, svc.port,
        "  (all interfaces)" if is_any_address(host) else ""))
    try:
        svc.httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        svc.close()
        try:
            os.remove(info_path)
        except OSError:
            pass
        svc.log("%s knxproject-mcp stopped" % _utc_now())
    return 0


def health_check(data_dir, timeout=4.0):
    info = _load_json(os.path.join(os.path.abspath(data_dir), "server.json"))
    if not info:
        print("knxproject-mcp: no running server found (server.json missing)", file=sys.stderr)
        return 1
    host, port = info.get("host") or "", int(info.get("port") or 0)
    ip = _ip(host) if host else None
    if not host or (ip is not None and ip.is_unspecified):
        host = "::1" if ip is not None and ip.version == 6 else "127.0.0.1"
    url = "http://%s:%d/healthz" % ("[%s]" % host if ":" in host else host, port)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(url, timeout=timeout) as r:
            ok = r.status == 200 and json.loads(r.read()).get("ok") is True
    except (OSError, ValueError):
        ok = False
    if not ok:
        print("knxproject-mcp: %s is not healthy" % url, file=sys.stderr)
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# configuration and CLI
# ---------------------------------------------------------------------------

def _env(name):
    v = os.environ.get(name)
    return v.strip() if v and v.strip() else None


def _env_list(name):
    v = _env(name)
    return [x.strip() for x in v.split(",") if x.strip()] if v else []


IMPORT_FORMATS = ("ets3", "esf", "inventory", "observe", "ha-yaml", "fhem", "claims")


def build_parser():
    ap = argparse.ArgumentParser(
        prog="knxproject_mcp.py",
        description="MCP server that keeps the record of a KNX installation. Every serve "
                    "option falls back to an environment variable.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    ps = sub.add_parser("serve", help="serve MCP over HTTP")
    pi = sub.add_parser("init", help="create an installation")
    pm = sub.add_parser("import", help="import data into an installation")
    ph = sub.add_parser("health", help="exit 0 if the running server answers /healthz")
    pb = sub.add_parser("backup", help="write a consistent, gzipped copy of the database")
    pc = sub.add_parser("stick-check", help="find the installation's KNX stick and check "
                                            "that it answers (no bus traffic)")
    for p in (ps, pi, pm, ph, pb, pc):
        p.add_argument("--data", help="data directory (KNXPROJECT_DATA, default %s)" % DEFAULT_DATA)
    pi.add_argument("--installation", required=True, help="id: lower-case, digits, - and _")
    pb.add_argument("dest", help="directory for the copies")
    pb.add_argument("--keep-days", type=int, default=14,
                    help="remove copies older than this (default 14)")
    pc.add_argument("--installation", help="installation id (needed if there are several)")
    pi.add_argument("--name", required=True)
    pm.add_argument("format", choices=IMPORT_FORMATS)
    pm.add_argument("paths", nargs="+")
    pm.add_argument("--installation", required=True)
    pm.add_argument("--as-of", help="date the data describes (default: from the data or "
                                    "the file)")
    pm.add_argument("--project", help="ets3: project name or id")
    pm.add_argument("--source", help="claims: source of the claims")
    pm.add_argument("--by", help="who imports (journal)")
    ps.add_argument("--bind", help="listen address, or 'auto' (KNXPROJECT_BIND, default %s)"
                                   % DEFAULT_BIND)
    ps.add_argument("--port", type=int, help="port (KNXPROJECT_PORT, default %d)" % DEFAULT_PORT)
    ps.add_argument("--allowed-host", action="append",
                    help="extra host name clients may use (KNXPROJECT_ALLOWED_HOSTS)")
    ps.add_argument("--allow-any-bind", action="store_true",
                    help="allow listening on 0.0.0.0 / :: (KNXPROJECT_ALLOW_ANY_BIND=1)")
    ps.add_argument("--no-mdns", action="store_true",
                    help="do not announce via mDNS/DNS-SD (KNXPROJECT_MDNS=0)")
    ps.add_argument("--mdns-name", help="DNS-SD instance name (KNXPROJECT_MDNS_NAME)")
    ps.add_argument("--mdns-hostname", help="announced host name (KNXPROJECT_MDNS_HOSTNAME)")
    ps.add_argument("--mdns-address", action="append",
                    help="IPv4 address to announce (KNXPROJECT_MDNS_ADDRESS)")
    return ap


def resolve_serve_config(args):
    cfg = {"cmd": "serve", "data": args.data or _env("KNXPROJECT_DATA") or DEFAULT_DATA}
    cfg["bind"] = args.bind or _env("KNXPROJECT_BIND") or DEFAULT_BIND
    port = args.port if args.port is not None else _env("KNXPROJECT_PORT") or DEFAULT_PORT
    try:
        port = int(port)
    except ValueError:
        raise ConfigError("port must be a number: %r" % port)
    if not 0 <= port <= 65535:
        raise ConfigError("port out of range: %d" % port)
    cfg["port"] = port
    hosts = args.allowed_host if args.allowed_host else _env_list("KNXPROJECT_ALLOWED_HOSTS")
    cfg["allowed_hosts"] = [normalize_host_entry(h) for h in hosts]
    cfg["allow_any_bind"] = bool(args.allow_any_bind) or (
        (_env("KNXPROJECT_ALLOW_ANY_BIND") or "").lower() in ("1", "true", "yes", "on"))
    name = args.mdns_name or _env("KNXPROJECT_MDNS_NAME")
    if name is not None and not 0 < len(name.strip().encode("utf-8")) <= 63:
        raise ConfigError("mDNS instance name must have 1..63 bytes: %r" % name)
    hostname = args.mdns_hostname or _env("KNXPROJECT_MDNS_HOSTNAME")
    if hostname is not None:
        h = hostname.strip().rstrip(".")
        if h.lower().endswith(".local"):
            h = h[:-6]
        if not re.match(r"^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$", h):
            raise ConfigError("mDNS host name must be one DNS label: %r" % hostname)
        hostname = h
    raw = args.mdns_address if args.mdns_address else _env_list("KNXPROJECT_MDNS_ADDRESS")
    addresses = []
    for a in (x.strip() for v in raw for x in v.split(",")):
        if not a:
            continue
        try:
            addresses.append(str(ipaddress.IPv4Address(a)))
        except ValueError:
            raise ConfigError("mDNS address must be an IPv4 address: %r" % a)
    cfg["mdns"] = {"enabled": not args.no_mdns and (_env("KNXPROJECT_MDNS") or "1").lower()
                   not in ("0", "false", "no", "off"),
                   "name": name.strip() if name else None, "hostname": hostname,
                   "address": addresses or None}
    return cfg


def _dated(as_of, seen, path):
    """-> (timestamp, date_from): --as-of first, then the date the input states,
    then the file's modification time."""
    if as_of:
        return as_of, "given"
    if seen:
        return seen, "data"
    return _file_date(path), "file"


BACKUP_RE = re.compile(r"^project-(\d{8})-\d{6}\.sqlite\.gz$")


def backup_store(store, dest, keep_days=14, now=None):
    """A consistent copy of the database, gzipped, written atomically into dest and
    read back; copies older than keep_days are removed.  -> facts."""
    now = now or datetime.datetime.now()
    os.makedirs(dest, exist_ok=True)
    tmp_db = store.path + ".backup.tmp"
    if os.path.exists(tmp_db):
        os.remove(tmp_db)
    try:
        with store.reader() as con:
            dst = sqlite3.connect(tmp_db)
            try:
                con.backup(dst)
                check = dst.execute("PRAGMA integrity_check").fetchone()[0]
            finally:
                dst.close()
        if check != "ok":
            raise StoreError("the copy failed its integrity check: %s" % check)
        with open(tmp_db, "rb") as f:
            raw = f.read()
    finally:
        if os.path.exists(tmp_db):
            os.remove(tmp_db)
    blob = gzip.compress(raw, 6)
    path = os.path.join(dest, "project-%s.sqlite.gz" % now.strftime("%Y%m%d-%H%M%S"))
    for _attempt in range(3):
        tmp = "%s.%d.tmp" % (path, os.getpid())
        with open(tmp, "wb") as f:
            f.write(blob)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        _fsync_dir(dest)
        with open(path, "rb") as f:
            back = f.read()
        try:
            if gzip.decompress(back) == raw:
                break
        except (OSError, EOFError):
            pass
    else:
        raise StoreError("%s does not read back intact" % path)
    cutoff = (now - datetime.timedelta(days=keep_days)).strftime("%Y%m%d")
    removed = []
    for name in sorted(os.listdir(dest)):
        m = BACKUP_RE.match(name)
        if m and m.group(1) < cutoff:
            os.remove(os.path.join(dest, name))
            removed.append(name)
    return {"path": path, "bytes": len(blob), "database_bytes": len(raw), "removed": removed}


def run_import(store, fmt, paths, inst, as_of=None, project=None, source=None, by=None):
    """-> list of (path, stats)."""
    results = []
    if fmt == "fhem":
        dated = []
        for p in paths:
            dm = _FHEM_DATE_RE.search(os.path.basename(p))
            dated.append((dm.group(1) if dm else norm_ts(as_of) or _file_date(p), p))
        for _d, p in sorted(dated):
            claims, seen, ref = import_fhem(p)
            # a backup's own date wins over --as-of: several files, one time line
            ts, date_from = _dated(None, seen, p) if seen else _dated(as_of, None, p)
            st = record_import(store, inst, "fhem", "fhem", ref, ts, claims,
                               lambda k, key, a: True, by, date_from)
            results.append((p, st))
        return results
    if len(paths) != 1:
        raise ImportFailed("%s takes one path" % fmt)
    p = paths[0]
    everything = lambda k, key, a: True  # noqa: E731
    if fmt == "ets3":
        claims, seen, ref = import_ets3(p, project)
        ts, date_from = _dated(as_of, seen, p)
        st = record_import(store, inst, "ets", "ets3", ref, ts, claims, everything, by, date_from)
    elif fmt == "esf":
        claims, ref = import_esf(p)
        ts, date_from = _dated(as_of, None, p)
        st = record_import(store, inst, "esf", "esf", ref, ts, claims, everything, by, date_from)
    elif fmt == "inventory":
        claims, scope, seen, ref = import_inventory(p)
        ts, date_from = _dated(as_of, seen, p)
        st = record_import(store, inst, "bus", "inventory", ref, ts, claims, scope, by, date_from)
    elif fmt == "observe":
        claims, ref = import_observe(p)
        ts, date_from = _dated(as_of, None, p)
        st = record_import(store, inst, "observed", "observe", ref, ts, claims, None, by,
                           date_from)
    elif fmt == "ha-yaml":
        claims, ref = import_ha_yaml(p)
        ts, date_from = _dated(as_of, None, p)
        st = record_import(store, inst, "ha", "ha-yaml", ref, ts, claims, everything, by,
                           date_from)
    elif fmt == "claims":
        if source not in SOURCES:
            raise ImportFailed("claims need --source: %s" % ", ".join(DEFAULT_PRECEDENCE))
        claims, scope, ref = import_claims_jsonl(p)
        ts, date_from = _dated(as_of, None, p)
        st = record_import(store, inst, source, "claims", ref, ts, claims, scope, by, date_from)
    else:
        raise ImportFailed("unknown format %r" % fmt)
    results.append((p, st))
    return results


def main(argv=None):
    args = build_parser().parse_args(argv)
    data = os.path.abspath(args.data or _env("KNXPROJECT_DATA") or DEFAULT_DATA)
    try:
        if args.cmd == "serve":
            return serve(resolve_serve_config(args))
        if args.cmd == "health":
            return health_check(data)
        store = Store(data)
        if args.cmd == "backup":
            r = backup_store(store, os.path.abspath(args.dest), args.keep_days)
            print("%s: %d bytes (database %d)%s" % (
                r["path"], r["bytes"], r["database_bytes"],
                ", removed %s" % " ".join(r["removed"]) if r["removed"] else ""))
            return 0
        if args.cmd == "stick-check":
            with store.reader() as con:
                inst = _inst(con, {"installation": args.installation})
                res = resolve(load_claims(con, inst, kind="installation", keys=["-"]),
                              precedence_of(con, inst))
            try:
                _url, facts = stick_check(value_of(res, "stick_id"))
            except StickError as e:
                print("knxproject-mcp: %s" % e, file=sys.stderr)
                return 1
            print(_dumps(facts))
            return 0
        if args.cmd == "init":
            create_installation(store, args.installation, args.name)
            print("installation %s created in %s" % (args.installation, store.path))
            return 0
        for p, st in run_import(store, args.format, args.paths, args.installation,
                                as_of=args.as_of, project=args.project, source=args.source,
                                by=args.by):
            print("%s: %d claims (%d new, %d reopened, %d ended), as of %s%s" % (
                p, st["asserted"], st["new"], st["reopened"], st["ended"], st["seen"],
                " (file date, pass --as-of if you know better)"
                if st.get("date_from") == "file" else ""))
        return 0
    except (ConfigError, StoreError, ImportFailed) as e:
        print("knxproject-mcp: %s" % e, file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
