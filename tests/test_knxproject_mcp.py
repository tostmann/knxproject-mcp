"""Tests for knxproject-mcp.  Every fixture is synthetic and built here."""

import csv
import datetime
import gzip
import http.client
import io
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import knxproject_mcp as k  # noqa: E402


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

def write_table(d, name, rows, cols=None):
    cols = cols or list(rows[0].keys())
    with gzip.open(os.path.join(d, "data.delta.%s.csv.gz" % name), "wt", encoding="utf-8",
                   newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r.get(c, "") for c in cols])


def make_ets3_dump(d):
    """Two projects; 'Demo' has area 1, line 1.2 with three devices."""
    write_table(d, "PROJECT", [{"PROJECT_ID": "1", "PROJECT_NAME": "Demo"},
                               {"PROJECT_ID": "2", "PROJECT_NAME": "Other"}])
    write_table(d, "MEDIUM_TYPE", [{"MEDIUM_TYPE_NUMBER": "0", "MEDIUM_TYPE_SHORT_NAME": "TP"}])
    write_table(d, "AREA", [{"AREA_ID": "10", "PROJECT_ID": "1", "AREA_ADDRESS": "1",
                             "AREA_NAME": "House", "AREA_DESCRIPTION": ""},
                            {"AREA_ID": "20", "PROJECT_ID": "2", "AREA_ADDRESS": "5",
                             "AREA_NAME": "Elsewhere", "AREA_DESCRIPTION": ""}])
    write_table(d, "LINE", [{"LINE_ID": "11", "AREA_ID": "10", "LINE_ADDRESS": "2",
                             "LINE_NAME": "Ground floor", "LINE_DESCRIPTION": "",
                             "MEDIUM_TYPE_NUMBER": "0"},
                            {"LINE_ID": "21", "AREA_ID": "20", "LINE_ADDRESS": "1",
                             "LINE_NAME": "x", "LINE_DESCRIPTION": "", "MEDIUM_TYPE_NUMBER": "0"}])
    dev_cols = ["DEVICE_ID", "LINE_ID", "PROJECT_ID", "CATALOG_ENTRY_ID", "PROGRAM_ID",
                "DEVICE_ADDRESS", "DEVICE_DESCRIPTION", "Name", "Comment",
                "BAU_SERIAL_NUMBER_HIGH", "BAU_SERIAL_NUMBER_LOW"]
    write_table(d, "DEVICE", [
        {"DEVICE_ID": "100", "LINE_ID": "11", "PROJECT_ID": "1", "CATALOG_ENTRY_ID": "500",
         "PROGRAM_ID": "600", "DEVICE_ADDRESS": "3", "DEVICE_DESCRIPTION": "Kitchen board"},
        {"DEVICE_ID": "101", "LINE_ID": "11", "PROJECT_ID": "1", "CATALOG_ENTRY_ID": "501",
         "PROGRAM_ID": "601", "DEVICE_ADDRESS": "8"},
        {"DEVICE_ID": "102", "LINE_ID": "11", "PROJECT_ID": "1", "CATALOG_ENTRY_ID": "500",
         "PROGRAM_ID": "600", "DEVICE_ADDRESS": "9"},
        {"DEVICE_ID": "200", "LINE_ID": "21", "PROJECT_ID": "2", "CATALOG_ENTRY_ID": "500",
         "PROGRAM_ID": "600", "DEVICE_ADDRESS": "1"}], dev_cols)
    write_table(d, "APPLICATION_PROGRAM", [
        {"PROGRAM_ID": "600", "MASK_ID": "70", "PROGRAM_NAME": "Switch 1234/1.0",
         "PROGRAM_VERSION": "16", "DEVICE_TYPE": str(0x1234), "MANUFACTURER_ID": "65520"},
        {"PROGRAM_ID": "601", "MASK_ID": "71", "PROGRAM_NAME": "Inputs 0ABC",
         "PROGRAM_VERSION": "1", "DEVICE_TYPE": str(0x0ABC), "MANUFACTURER_ID": "240"}])
    write_table(d, "CATALOG_ENTRY", [
        {"CATALOG_ENTRY_ID": "500", "ORDER_NUMBER": "SW-8", "ENTRY_NAME": "Switch actuator 8x"},
        {"CATALOG_ENTRY_ID": "501", "ORDER_NUMBER": "BI-4", "ENTRY_NAME": "Binary input 4x"}])
    write_table(d, "MANUFACTURER", [{"MANUFACTURER_ID": "65520", "MANUFACTURER_NAME": "Test Co"},
                                    {"MANUFACTURER_ID": "240", "MANUFACTURER_NAME": "Old Co"}])
    write_table(d, "MASK", [{"MASK_ID": "70", "MASK_VERSION": str(0x0701)},
                            {"MASK_ID": "71", "MASK_VERSION": str(0x0012)}])
    write_table(d, "DEVICE_PROGRAMMING", [
        {"DEVICE_ID": "100", "LAST_MODIFIED": "2015-05-29 12:25:14",
         "LAST_DOWNLOAD": "2015-05-29 12:25:40"}])
    write_table(d, "MAINGROUP", [{"MAINGROUP_ID": "30", "PROJECT_ID": "1",
                                  "MAINGROUP_ADDRESS": "3", "MAINGROUP_NAME": "Home"}])
    write_table(d, "MIDDLEGROUP", [{"MIDDLEGROUP_ID": "31", "MAINGROUP_ID": "30",
                                    "MIDDLEGROUP_ADDRESS": "1", "MIDDLEGROUP_NAME": "Light"},
                                   {"MIDDLEGROUP_ID": "32", "MAINGROUP_ID": "30",
                                    "MIDDLEGROUP_ADDRESS": "2", "MIDDLEGROUP_NAME": "Doors"}])
    write_table(d, "SUBGROUP", [
        {"SUBGROUP_ID": "40", "MIDDLEGROUP_ID": "31", "SUBGROUP_ADDRESS": "7",
         "SUBGROUP_NAME": "Garden", "SUBGROUP_DESCRIPTION": "", "Comment": "",
         "SUBGROUP_CENTRAL": "", "SUBGROUP_PASSTHROUGH": ""},
        {"SUBGROUP_ID": "41", "MIDDLEGROUP_ID": "31", "SUBGROUP_ADDRESS": "8",
         "SUBGROUP_NAME": "Porch", "SUBGROUP_DESCRIPTION": "front", "Comment": "",
         "SUBGROUP_CENTRAL": "1", "SUBGROUP_PASSTHROUGH": ""},
        {"SUBGROUP_ID": "42", "MIDDLEGROUP_ID": "32", "SUBGROUP_ADDRESS": "1",
         "SUBGROUP_NAME": "Gate", "SUBGROUP_DESCRIPTION": "", "Comment": "",
         "SUBGROUP_CENTRAL": "", "SUBGROUP_PASSTHROUGH": ""}])
    write_table(d, "OBJECT_TYPE", [{"OBJECT_TYPE_CODE": "0", "OBJECT_TYPE_NAME": "1 Bit"},
                                   {"OBJECT_TYPE_CODE": "7", "OBJECT_TYPE_NAME": "1 Byte"}])
    write_table(d, "OBJECT_PRIORITY", [{"OBJECT_PRIORITY_CODE": "3", "OBJECT_PRIORITY_NAME": "Low"}])
    oc = ["DEVICE_ID", "DEVICE_OBJECT_ID", "OBJECT_ID", "DEVICE_OBJECT_NUMBER",
          "DEVICE_OBJECT_VISIBLE", "DEVICE_OBJECT_TYPE", "OBJECT_PRIO", "OBJECT_COMM",
          "OBJECT_READ", "OBJECT_WRITE", "OBJECT_TRANS", "OBJECT_UPDATE", "OBJECT_READONINIT",
          "EIB_DATA_TYPE_CODE", "EIB_DATA_SUBTYPE_CODE", "Name", "Description"]
    write_table(d, "DEVICE_OBJECT", [
        {"DEVICE_ID": "100", "DEVICE_OBJECT_ID": "700", "OBJECT_ID": "800",
         "DEVICE_OBJECT_NUMBER": "0", "DEVICE_OBJECT_VISIBLE": "1", "DEVICE_OBJECT_TYPE": "0",
         "OBJECT_PRIO": "3", "OBJECT_COMM": "1", "OBJECT_WRITE": "1"},
        {"DEVICE_ID": "100", "DEVICE_OBJECT_ID": "701", "OBJECT_ID": "801",
         "DEVICE_OBJECT_NUMBER": "1", "DEVICE_OBJECT_VISIBLE": "0", "DEVICE_OBJECT_TYPE": "0"},
        {"DEVICE_ID": "100", "DEVICE_OBJECT_ID": "702", "OBJECT_ID": "802",
         "DEVICE_OBJECT_NUMBER": "2", "DEVICE_OBJECT_VISIBLE": "0", "DEVICE_OBJECT_TYPE": "0",
         "OBJECT_COMM": "1", "OBJECT_TRANS": "1", "OBJECT_READ": "1"},
        {"DEVICE_ID": "101", "DEVICE_OBJECT_ID": "703", "OBJECT_ID": "803",
         "DEVICE_OBJECT_NUMBER": "2", "DEVICE_OBJECT_VISIBLE": "1", "DEVICE_OBJECT_TYPE": "0",
         "OBJECT_COMM": "1", "OBJECT_TRANS": "1"}], oc)
    write_table(d, "SUBGROUP_TO_OBJECT", [
        {"SUBGROUP_ID": "40", "DEVICE_OBJECT_ID": "700", "SEND": "1"},
        {"SUBGROUP_ID": "41", "DEVICE_OBJECT_ID": "700", "SEND": "0"},
        {"SUBGROUP_ID": "41", "DEVICE_OBJECT_ID": "702", "SEND": "1"},
        {"SUBGROUP_ID": "42", "DEVICE_OBJECT_ID": "703", "SEND": "1"}])
    write_table(d, "COMMUNICATION_OBJECT", [
        {"OBJECT_ID": "800", "OBJECT_NAME": "Switch", "OBJECT_FUNCTION": "Channel A",
         "OBJECT_TYPE": "0"},
        {"OBJECT_ID": "801", "OBJECT_NAME": "Hidden", "OBJECT_FUNCTION": "Channel A"},
        {"OBJECT_ID": "802", "OBJECT_NAME": "Status", "OBJECT_FUNCTION": "Channel A"},
        {"OBJECT_ID": "803", "OBJECT_NAME": "Input", "OBJECT_FUNCTION": "Input 1"}])
    write_table(d, "DEVICE_PARAMETER", [
        {"DEVICE_ID": "100", "PARAMETER_ID": "900", "DEVICE_PARAMETER_VISIBLE": "1",
         "PARAMETER_VALUE_LONG": "3"},
        {"DEVICE_ID": "100", "PARAMETER_ID": "901", "DEVICE_PARAMETER_VISIBLE": "1",
         "PARAMETER_VALUE_LONG": "0"},
        {"DEVICE_ID": "100", "PARAMETER_ID": "902", "DEVICE_PARAMETER_VISIBLE": "1",
         "PARAMETER_VALUE_LONG": "2"},
        {"DEVICE_ID": "100", "PARAMETER_ID": "903", "DEVICE_PARAMETER_VISIBLE": "0",
         "PARAMETER_VALUE_LONG": "9"},
        {"DEVICE_ID": "100", "PARAMETER_ID": "904", "DEVICE_PARAMETER_VISIBLE": "1",
         "PARAMETER_VALUE_LONG": "0"}])
    pc = ["PROGRAM_ID", "PARAMETER_ID", "PARAMETER_NUMBER", "PARAMETER_NAME",
          "PARAMETER_DESCRIPTION", "PARAMETER_TYPE_ID", "PAR_PARAMETER_ID", "PARAMETER_ADDRESS",
          "PARAMETER_BITOFFSET", "PARAMETER_SIZE", "PARAMETER_DEFAULT_LONG",
          "PARAMETER_DEFAULT_STRING"]
    write_table(d, "PARAMETER", [
        {"PROGRAM_ID": "600", "PARAMETER_ID": "899", "PARAMETER_NUMBER": "1",
         "PARAMETER_DESCRIPTION": "A: Switching", "PARAMETER_TYPE_ID": "50"},
        {"PROGRAM_ID": "600", "PARAMETER_ID": "900", "PARAMETER_NUMBER": "61",
         "PARAMETER_DESCRIPTION": "Staircase   time\nbase", "PARAMETER_TYPE_ID": "51",
         "PAR_PARAMETER_ID": "899", "PARAMETER_ADDRESS": str(0x4A2A),
         "PARAMETER_BITOFFSET": "2", "PARAMETER_SIZE": "3", "PARAMETER_DEFAULT_LONG": "4"},
        {"PROGRAM_ID": "600", "PARAMETER_ID": "901", "PARAMETER_NUMBER": "62",
         "PARAMETER_DESCRIPTION": "Page", "PARAMETER_TYPE_ID": "52"},
        {"PROGRAM_ID": "600", "PARAMETER_ID": "902", "PARAMETER_NUMBER": "63",
         "PARAMETER_DESCRIPTION": "Number of scenes", "PARAMETER_TYPE_ID": "53",
         "PARAMETER_DEFAULT_LONG": "2"},
        {"PROGRAM_ID": "600", "PARAMETER_ID": "903", "PARAMETER_NUMBER": "64",
         "PARAMETER_DESCRIPTION": "Invisible", "PARAMETER_TYPE_ID": "53"},
        {"PROGRAM_ID": "600", "PARAMETER_ID": "904", "PARAMETER_NUMBER": "65",
         "PARAMETER_DESCRIPTION": "Heading", "PARAMETER_TYPE_ID": "54"}], pc)
    write_table(d, "PARAMETER_TYPE", [
        {"PARAMETER_TYPE_ID": "50", "ATOMIC_TYPE_NUMBER": "4"},
        {"PARAMETER_TYPE_ID": "51", "ATOMIC_TYPE_NUMBER": "4"},
        {"PARAMETER_TYPE_ID": "52", "ATOMIC_TYPE_NUMBER": "4"},
        {"PARAMETER_TYPE_ID": "53", "ATOMIC_TYPE_NUMBER": "1"},
        {"PARAMETER_TYPE_ID": "54", "ATOMIC_TYPE_NUMBER": "0"}])
    write_table(d, "PARAMETER_LIST_OF_VALUES", [
        {"PARAMETER_TYPE_ID": "51", "REAL_VALUE": "3", "DISPLAYED_VALUE": "1 s"},
        {"PARAMETER_TYPE_ID": "51", "REAL_VALUE": "4", "DISPLAYED_VALUE": "1 min"},
        {"PARAMETER_TYPE_ID": "52", "REAL_VALUE": "0", "DISPLAYED_VALUE": "Dummy"}])


ESF = ("Demo\n"
       "Home.Light.3/1/7\tGarden lamp\tEIS 1 'Switching' (1 Bit)\tLow\t\n"
       "Home.Light.3/1/8\tPorch\tEIS 1 'Switching' (1 Bit)\tLow\t\n"
       "Home.Doors.3/2/1\tGate \xe4\tEIS 1 'Switching' (1 Bit)\tLow\t\n")

INVENTORY = {
    "lines": [{"from": "1.2.0", "to": "1.2.255", "probed": 256}],
    "devices": [
        {"address": "1.2.3", "mask": "0x0701", "application_id": "FFF0123410",
         "serial_number": "FFF000000001",
         "lsm": [{"idx": 1, "name": "Loaded"}, {"idx": 2, "name": "Loaded"}],
         "associations": [{"tsap": 1, "ga": "3/1/7", "com_obj": 0},
                          {"tsap": 2, "ga": "3/1/8", "com_obj": 2}]},
        {"address": "1.2.20", "mask": "0x0701", "application_id": "FFF0123410",
         "associations": [{"tsap": 1, "ga": "3/1/9", "com_obj": 4}]}]}

HA_YAML = """\
homeassistant:
  name: x
knx:
  light:
    - name: "Garden light"
      address: "3/1/7"
      state_address: "3/1/7"
  cover:
    - name: Shutter   # a comment
      move_long_address: "3/3/1"
      stop_address: '3/3/2'
  button:
    - name: "Gate open"
      address: "3/2/1"
      payload: 1
  binary_sensor:
    - name: Sensors
      state_address:
        - "3/4/1"
        - "3/4/2"
sensor:
  - platform: other
"""

FHEM_OLD = ("define EIB.Light.Garden EIB 3/1/7\n"
            "define EIB.Old.Thing EIB 3/1/5\n")
FHEM_NEW = ("define EIB.Light.Garden EIB 3/1/7\n"
            "define EIB_3209 EIB 3209\n"
            "attr EIB.Light.Garden room Garden\n")


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="knxproject-test-")
        self.data = os.path.join(self.tmp, "data")
        self.store = k.Store(self.data)
        k.create_installation(self.store, "demo", "Demo house")
        self.dump = os.path.join(self.tmp, "dump")
        os.makedirs(self.dump)
        make_ets3_dump(self.dump)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def path(self, name, text, encoding="utf-8"):
        p = os.path.join(self.tmp, name)
        with open(p, "wb") as f:
            f.write(text.encode(encoding) if isinstance(text, str) else text)
        return p

    def imp(self, fmt, paths, **kw):
        if isinstance(paths, str):
            paths = [paths]
        return k.run_import(self.store, fmt, paths, "demo", **kw)

    def load_all(self):
        self.imp("ets3", self.dump, project="Demo")
        self.imp("esf", self.path("x.esf", ESF, "cp1252"), as_of="2020-01-01")
        self.imp("inventory", self.path("inv.json", json.dumps(INVENTORY)), as_of="2026-08-01")
        self.imp("ha-yaml", self.path("ha.yaml", HA_YAML), as_of="2026-08-02")
        self.imp("fhem", [self.path("fhem.cfg.2014-01-01", FHEM_OLD),
                          self.path("fhem.cfg.2024-11-15", FHEM_NEW)])

    def call(self, name, args=None, mid=1):
        r = k.handle_rpc({"jsonrpc": "2.0", "id": mid, "method": "tools/call",
                          "params": {"name": name, "arguments": args or {}}},
                         self.store, caller="test")
        self.assertLessEqual(len(json.dumps(r, ensure_ascii=False).encode()), k.HARD_REPLY_CAP)
        res = r["result"]
        return json.loads(res["content"][0]["text"]), res["isError"]

    def ok(self, name, args=None):
        out, err = self.call(name, args)
        self.assertFalse(err, out)
        return out


# ---------------------------------------------------------------------------
# addresses and references
# ---------------------------------------------------------------------------

class TestAddresses(unittest.TestCase):
    def test_individual(self):
        self.assertEqual(k.norm_ia(" 1.2.3 "), "1.2.3")
        for bad in ("16.1.1", "1.16.1", "1.1.256", "1.1", "a.b.c", None):
            self.assertIsNone(k.norm_ia(bad))

    def test_group(self):
        self.assertEqual(k.norm_ga("31/7/255"), "31/7/255")
        self.assertIsNone(k.norm_ga("3/8/1"))
        self.assertIsNone(k.norm_ga("32/1/1"))
        self.assertEqual(k.norm_ga(k.ga_int("3/1/7")), "3/1/7")
        self.assertEqual(k.ga_from_int(0x1907), "3/1/7")

    def test_fhem_hex_is_4_4_8(self):
        self.assertEqual(k.fhem_hex_ga("3107"), "3/1/7")
        self.assertEqual(k.fhem_hex_ga("b406"), "11/4/6")
        self.assertIsNone(k.fhem_hex_ga("b4"))

    def test_refs(self):
        P = k.parse_ref
        self.assertEqual(P("1.2.3"), ("device", "1.2.3"))
        self.assertEqual(P("1.2"), ("line", "1.2"))
        self.assertEqual(P("3/1/7"), ("group", "3/1/7"))
        self.assertEqual(P("3/1"), ("range", "3/1"))
        self.assertEqual(P("range:3"), ("range", "3"))
        self.assertEqual(P("area:1"), ("area", "1"))
        self.assertEqual(P("1.2.3#024"), ("object", "1.2.3#24"))
        self.assertEqual(P("1.2.3#24>3/1/7"), ("link", "1.2.3#24>3/1/7"))
        self.assertEqual(P("1.2.3:P61"), ("param", "1.2.3:P61"))
        self.assertEqual(P("installation"), ("installation", "-"))
        for bad in ("", "3/8/1", "1.2.3#x", "1.2.3#1>9/9/9", "hello", "area:99"):
            with self.assertRaises(k.RefError):
                P(bad)

    def test_app_id(self):
        self.assertEqual(k.ets3_app_id(0x0C, 0x1285, 1, 0x0012), "0C128501")
        self.assertEqual(k.ets3_app_id(0x0C, 0x5701, 0x10, 0x0701), "000C570110")
        self.assertIsNone(k.ets3_app_id(None, 1, 1, 0x0701))

    def test_ts(self):
        self.assertEqual(k.norm_ts("2015-05-29 12:25:14"), "2015-05-29T12:25:14")
        self.assertEqual(k.norm_ts("2015-05-29"), "2015-05-29T00:00:00")
        self.assertIsNone(k.norm_ts("2015-13-01"))
        self.assertIsNone(k.norm_ts("junk"))


# ---------------------------------------------------------------------------
# imports
# ---------------------------------------------------------------------------

class TestEts3(Base):
    def test_project_must_be_chosen(self):
        with self.assertRaises(k.ImportFailed) as cm:
            self.imp("ets3", self.dump)
        self.assertIn("Demo", str(cm.exception))
        with self.assertRaises(k.ImportFailed):
            self.imp("ets3", self.dump, project="Nope")

    def test_content(self):
        claims, seen, ref = k.import_ets3(self.dump, "demo")
        c = {(a, b, attr): v for a, b, attr, v, _r in claims}
        self.assertEqual(seen, "2015-05-29T12:25:40")
        self.assertEqual(c[("device", "1.2.3", "app_id")], "FFF0123410")
        self.assertEqual(c[("device", "1.2.8", "app_id")], "F00ABC01")      # BCU1: 8 hex
        self.assertEqual(c[("device", "1.2.3", "description")], "Kitchen board")
        self.assertEqual(c[("device", "1.2.3", "order_number")], "SW-8")
        self.assertEqual(c[("device", "1.2.3", "manufacturer")], "Test Co")
        self.assertNotIn(("device", "5.1.1", "present"), c)                 # other project
        self.assertEqual(c[("line", "1.2", "medium")], "TP")
        self.assertEqual(c[("range", "3/2", "name")], "Doors")
        self.assertTrue(c[("group", "3/1/8", "central")])
        self.assertTrue(c[("link", "1.2.3#0>3/1/7", "send")])
        self.assertFalse(c[("link", "1.2.3#0>3/1/8", "send")])
        self.assertEqual(c[("object", "1.2.3#0", "flags")], "C-W---")
        self.assertEqual(c[("object", "1.2.3#0", "priority")], "Low")
        self.assertEqual(c[("group", "3/1/7", "size")], "1 Bit")
        self.assertNotIn(("object", "1.2.3#1", "name"), c)    # hidden and unlinked
        self.assertFalse(c[("object", "1.2.3#2", "visible")])  # hidden but linked
        p = "1.2.3:P61"
        self.assertEqual(c[("param", p, "name")], "Staircase time base")
        self.assertEqual(c[("param", p, "value")], 3)
        self.assertEqual(c[("param", p, "text")], "1 s")
        self.assertEqual(c[("param", p, "default")], 4)
        self.assertEqual(c[("param", p, "path")], "A: Switching")
        self.assertEqual(c[("param", p, "memory")],
                         {"address": "0x4A2A", "bit_offset": 2, "size_bits": 3})
        self.assertTrue(c[("param", "1.2.3:P63", "ets_only")])
        self.assertNotIn(("param", "1.2.3:P63", "default"), c)             # equals the value
        self.assertNotIn(("param", "1.2.3:P62", "value"), c)               # Dummy page
        self.assertNotIn(("param", "1.2.3:P64", "value"), c)               # invisible
        self.assertNotIn(("param", "1.2.3:P65", "value"), c)               # heading

    def test_reimport_is_idempotent_and_ends_what_disappeared(self):
        st = self.imp("ets3", self.dump, project="Demo")[0][1]
        self.assertGreater(st["new"], 40)
        st = self.imp("ets3", self.dump, project="Demo")[0][1]
        self.assertEqual((st["new"], st["ended"]), (0, 0))
        write_table(self.dump, "SUBGROUP_TO_OBJECT", [
            {"SUBGROUP_ID": "40", "DEVICE_OBJECT_ID": "700", "SEND": "1"}])
        st = self.imp("ets3", self.dump, project="Demo", as_of="2016-01-01")[0][1]
        self.assertGreater(st["ended"], 0)
        with self.store.reader() as con:
            rows = k.load_claims(con, "demo", kind="link", keys=["1.2.3#2>3/1/8"], ended=True)
        self.assertTrue(rows and all(r["ended_at"] == "2016-01-01T00:00:00" for r in rows))


class TestLifecycle(Base):
    def put(self, claims, seen, scope=lambda *a: True):
        with self.store.writer() as con:
            return k.put_claims(con, "demo", "ha", claims, seen, scope=scope)

    def test_end_reopen_and_older_imports(self):
        a = ("group", "3/1/7", "name", "A", None)
        b = ("group", "3/1/8", "name", "B", None)
        self.put([a, b], "2020-01-01")
        st = self.put([a], "2021-01-01")
        self.assertEqual(st["ended"], 1)
        st = self.put([a], "2019-01-01")                 # older: extends, ends nothing
        self.assertEqual(st["ended"], 0)
        st = self.put([a, b], "2022-01-01")
        self.assertEqual(st["reopened"], 1)
        with self.store.reader() as con:
            r = k.load_claims(con, "demo", keys=["3/1/7"])[0]
        self.assertEqual((r["first_seen"][:4], r["last_seen"][:4]), ("2019", "2022"))

    def test_precedence(self):
        self.load_all()
        with self.store.reader() as con:
            prec = k.precedence_of(con, "demo")
            res = k.resolve(k.load_claims(con, "demo", kind="group", keys=["3/1/7"]), prec)
        self.assertEqual(res["name"]["value"], "Garden light")       # ha beats fhem/ets/esf
        self.assertEqual(res["name"]["source"], "ha")
        self.assertEqual(res["name"]["differs"], ["esf", "ets", "fhem"])


class TestOtherImports(Base):
    def test_esf_cp1252(self):
        st = self.imp("esf", self.path("x.esf", ESF, "cp1252"))[0][1]
        self.assertGreater(st["new"], 5)
        out = self.ok("proj-group", {"ga": "3/2/1"})
        self.assertEqual(out["name"], "Gate ä")
        self.assertEqual(out["range"], "Home / Doors")

    def test_inventory_scope(self):
        self.imp("inventory", self.path("inv.json", json.dumps(INVENTORY)), as_of="2026-01-01")
        with self.store.writer() as con:     # a device on another line, also from the bus
            k.put_claims(con, "demo", "bus", [("device", "1.5.1", "present", True, None)],
                         "2026-01-01")
        inv = dict(INVENTORY, devices=INVENTORY["devices"][:1])
        st = self.imp("inventory", self.path("inv2.json", json.dumps(inv)),
                      as_of="2026-02-01")[0][1]
        self.assertEqual(st["ended"], 4)         # 1.2.20: present, mask, app_id, link
        with self.store.reader() as con:
            cur = {r["key"] for r in k.load_claims(con, "demo", kind="device", source="bus")}
        self.assertEqual(cur, {"1.2.3", "1.5.1"})

    def test_inventory_rpc_wrapper_and_rejects(self):
        st = self.imp("inventory", self.path("r.json", json.dumps({"result": INVENTORY})))[0][1]
        self.assertGreater(st["new"], 3)
        with self.assertRaises(k.ImportFailed):
            self.imp("inventory", self.path("bad.json", "{}"))

    def test_ha_yaml(self):
        plats = k.parse_ha_knx(HA_YAML)
        self.assertEqual(sorted(plats), ["binary_sensor", "button", "cover", "light"])
        self.assertEqual(plats["cover"][0]["stop_address"], "3/3/2")
        self.assertEqual(plats["cover"][0]["name"], "Shutter")
        self.assertEqual(plats["binary_sensor"][0]["state_address"], ["3/4/1", "3/4/2"])
        claims, _ref = k.import_ha_yaml(self.path("ha.yaml", HA_YAML))
        names = {(key, v) for kind, key, a, v, _r in claims if a == "name"}
        self.assertIn(("3/3/2", "Shutter (stop)"), names)
        self.assertIn(("3/4/2", "Sensors"), names)
        self.assertIn(("3/1/7", "Garden light"), names)
        with self.assertRaises(k.ImportFailed):
            k.parse_ha_knx("sensor:\n  - x: 1\n")

    def test_fhem_history(self):
        self.imp("fhem", [self.path("fhem.cfg.2024-11-15", FHEM_NEW),
                          self.path("fhem.cfg.2014-01-01", FHEM_OLD)])
        out = self.ok("proj-history", {"ref": "3/1/5"})
        c = [x for x in out["claims"] if x["attr"] == "name"][0]
        self.assertEqual(c["ended"], "2024-11-15")
        out = self.ok("proj-group", {"ga": "3/2/9"})
        auto = [x for x in out["claims"] if x["attr"] == "fhem_autocreate"]
        self.assertEqual(auto[0]["value"], "EIB_3209")
        self.assertNotIn("name", out)
        out = self.ok("proj-group", {"ga": "3/1/7"})
        c = [x for x in out["claims"] if x["attr"] == "name"][0]
        self.assertEqual(c["seen"], "2014-01-01..2024-11-15")

    def test_observe_and_claims(self):
        obs = {"what": "ga", "window_s": 60, "rows": [{"ga": "3/1/7", "count": 4, "svc": ["write"]}]}
        self.imp("observe", self.path("o.json", json.dumps(obs)), as_of="2026-08-01")
        out = self.ok("proj-group", {"ga": "3/1/7"})
        self.assertEqual(out["traffic"]["count"], 4)
        lines = [{"ref": "1.2.3#0", "attr": "slot_name", "value": "Slot name"},
                 {"kind": "device", "key": "1.2.3", "attr": "location", "value": "Cellar"}]
        p = self.path("c.jsonl", "\n".join(json.dumps(x) for x in lines))
        with self.assertRaises(k.ImportFailed):
            self.imp("claims", p)                              # no source
        self.imp("claims", p, source="product")
        out = self.ok("proj-device", {"address": "1.2.3", "section": "objects"})
        self.assertEqual(out["objects"][0]["slot_name"], "Slot name")
        self.assertNotIn("name", out["objects"][0])
        # a later file about other attributes of the same device ends nothing else
        self.imp("inventory", self.path("inv.json", json.dumps(INVENTORY)), as_of="2026-08-01")
        p2 = self.path("c2.jsonl", json.dumps({"ref": "1.2.3", "attr": "channel_modes",
                                               "value": ["K1: switch"]}))
        st = self.imp("claims", p2, source="bus", as_of="2026-08-02")[0][1]
        self.assertEqual(st["ended"], 0)
        dev = self.ok("proj-device", {"address": "1.2.3"})
        self.assertEqual((dev["app_id"], dev["channel_modes"]), ("FFF0123410", ["K1: switch"]))
        bad = self.path("b.jsonl", json.dumps({"ref": "1.2.3", "attr": "Bad Attr", "value": 1}))
        with self.assertRaises(k.ImportFailed):
            self.imp("claims", bad, source="product")


# ---------------------------------------------------------------------------
# tools
# ---------------------------------------------------------------------------

class TestTools(Base):
    def setUp(self):
        super().setUp()
        self.load_all()

    def test_brief(self):
        out = self.ok("proj-brief")
        self.assertEqual(out["installation"], {"id": "demo", "name": "Demo"})
        self.assertEqual(out["counts"]["devices"], 4)
        self.assertEqual({s["source"] for s in out["sources"]}, {"ets", "esf", "bus", "ha", "fhem"})
        self.assertEqual(out["pending_changes"], 0)
        self.assertEqual(out["rules"], [])
        self.assertIn("not recorded", out["bus_access"])
        self.assertEqual(out["decisions_by_user"], 0)

    def test_brief_dates_open_items_and_access(self):
        obs = {"what": "ga", "window_s": 60, "rows": [{"ga": "3/1/7", "count": 4}]}
        p = self.path("o.json", json.dumps(obs))
        os.utime(p, (1754000000, 1754000000))
        self.imp("observe", p)                                 # no --as-of: only the file time
        src = {s["source"]: s for s in self.ok("proj-brief")["sources"]}
        self.assertIsNone(src["observed"]["as_of"])
        self.assertEqual(src["observed"]["file_date"],
                         datetime.datetime.fromtimestamp(1754000000).strftime("%Y-%m-%d"))
        self.assertEqual(src["esf"]["as_of"], "2020-01-01")   # --as-of given
        self.assertNotIn("file_date", src["esf"])
        self.ok("proj-set", {"ref": "installation", "attr": "description",
                             "value": "A house", "said": "describe it"})
        self.ok("proj-set", {"ref": "installation", "attr": "bus_access",
                             "value": "stick 1.2.250", "said": "that stick"})
        brief = self.ok("proj-brief")
        self.assertEqual(brief["bus_access"], "stick 1.2.250")
        self.assertEqual(brief["decisions_by_user"], 0)       # the record's own text
        self.ok("proj-set", {"ref": "3/1/7", "attr": "name", "value": "Lawn", "said": "s"})
        self.assertEqual(self.ok("proj-brief")["decisions_by_user"], 1)
        for i in range(3):
            self.ok("proj-note", {"kind": "finding", "text": "lead %d" % i})
        op = self.ok("proj-brief")["open"]
        self.assertEqual((len(op["items"]), "rest" in op), (3, False))
        old = k.BRIEF_OPEN_MAX
        k.BRIEF_OPEN_MAX = 2
        try:
            op = self.ok("proj-brief")["open"]
        finally:
            k.BRIEF_OPEN_MAX = old
        self.assertEqual((op["findings"], op["items_shown"]), (3, 2))
        self.assertIn("proj-note action=list", op["rest"])

    def test_brief_shortens_long_findings_before_dropping_any(self):
        for i in range(15):
            self.ok("proj-note", {"kind": "finding", "text": "%02d " % i + "y" * 1500})
        self.ok("proj-note", {"kind": "todo", "text": "read 1.2.3 again"})
        op = self.ok("proj-brief")["open"]
        self.assertEqual((len(op["items"]), op["texts_cut"]), (16, True))
        self.assertNotIn("items_shown", op)
        self.assertIn("proj-note action=list", op["rest"])
        self.assertLessEqual(max(len(it["text"]) for it in op["items"]), k.BRIEF_ITEM_CHARS + 1)
        for i in range(40):
            self.ok("proj-note", {"kind": "finding", "text": "%02d " % i + "z" * 1500})
        op = self.ok("proj-brief")["open"]
        self.assertEqual(op["findings"], 55)
        self.assertLessEqual(op["items_shown"], k.BRIEF_OPEN_MAX + 1)   # 20 findings + todo
        self.assertEqual(op["items"][-1]["kind"], "todo")          # the todo stays

    def test_note_and_history_pages_follow_next_offset(self):
        for i in range(60):
            self.ok("proj-note", {"kind": "note", "text": "%02d " % i + "x" * 400,
                                  "about": ["3/1/7"]})

        def walk(tool, args, key):
            seen, offset = [], 0
            for _ in range(50):
                out = self.ok(tool, dict(args, offset=offset))
                seen += out[key]
                if out["next_offset"] is None:
                    return seen, out["total"]
                self.assertGreater(out["next_offset"], offset)
                offset = out["next_offset"]
            self.fail("%s never reached the last page" % tool)

        notes, total = walk("proj-note", {"action": "list"}, "notes")
        self.assertEqual((len(notes), len({n["id"] for n in notes})), (total, total))
        self.assertEqual(total, 60)
        journal, total = walk("proj-history", {}, "journal")
        self.assertEqual(len(journal), total)
        self.assertGreaterEqual(total, 60)

    def test_group_detail_and_range(self):
        out = self.ok("proj-group", {"ga": "3/1/8"})
        self.assertEqual(out["name"], "Porch")
        self.assertTrue(out["central"])
        devs = {(x["device"], x["object"]): x["sources"] for x in out["links"]}
        self.assertEqual(devs[("1.2.3", 2)], ["bus", "ets"])
        self.assertEqual(devs[("1.2.3", 0)], ["ets"])
        out = self.ok("proj-group", {"ga": "3"})
        gas = [g["ga"] for g in out["groups"]]
        self.assertEqual(gas, sorted(gas, key=k.ga_int))
        self.assertIn("3/1/9", gas)                    # only known from a bus link
        self.assertEqual(out["range_names"]["3/1"], "Light")
        out, err = self.call("proj-group", {"ga": "x/y"})
        self.assertTrue(err)
        self.assertEqual(out["error"], "invalid_argument")

    def test_device(self):
        out = self.ok("proj-device", {"address": "1.2.3"})
        self.assertEqual(out["present"]["says_yes"], ["bus", "ets"])
        self.assertEqual(out["app_id"], "FFF0123410")
        self.assertEqual(out["load_state"], "1:Loaded 2:Loaded")
        self.assertEqual(out["line_name"], "Ground floor")
        out = self.ok("proj-device", {"address": "1.2.3", "section": "params",
                                      "query": "staircase"})
        self.assertEqual([p["ref"] for p in out["params"]], ["P61"])
        out, err = self.call("proj-device", {"address": "9.9.9"})
        self.assertTrue(err)
        self.assertEqual(out["error"], "unknown_device")

    def test_topology(self):
        out = self.ok("proj-topology", {"format": "mermaid"})
        self.assertIn("flowchart TD", out["mermaid"])
        self.assertIn("class D1_2_8,D1_2_9 unconfirmed", out["mermaid"])
        tree = self.ok("proj-topology")["tree"]
        line = tree[0]["lines"][0]
        self.assertEqual(line["line"], "1.2")
        self.assertEqual([d["address"] for d in line["devices"]], ["1.2.3", "1.2.8", "1.2.9", "1.2.20"])
        out = self.ok("proj-topology", {"view": "functional", "format": "mermaid", "range": "3/1"})
        self.assertIn("G3_1_7 --- D1_2_3", out["mermaid"])
        self.assertNotIn("3/2/1", out["mermaid"])

    def test_diff(self):
        out = self.ok("proj-diff")
        self.assertEqual(out["devices_only_in_ets"]["items"], ["1.2.8", "1.2.9"])
        self.assertEqual(out["devices_only_in_bus"]["items"], ["1.2.20"])
        self.assertEqual(out["links_only_in_ets"]["items"], ["1.2.3#0>3/1/8"])

    def test_find(self):
        out = self.ok("proj-find", {"query": "porch"})
        self.assertEqual(out["results"][0]["ref"], "3/1/8")
        out = self.ok("proj-find", {"query": "1.2.3"})
        self.assertEqual(out["results"][0]["ref"], "1.2.3")
        self.ok("proj-find", {"query": "x", "kinds": ["note"]})

    def test_brief_and_find_read_their_indexes(self):
        def plan(sql, params):
            with self.store.reader() as con:
                return " ".join(r[-1] for r in con.execute("EXPLAIN QUERY PLAN " + sql, params))
        brief = plan("SELECT count(DISTINCT key) FROM claims WHERE inst=? AND kind=? AND "
                     "ended_at IS NULL", ("demo", "param"))
        self.assertIn("COVERING INDEX claims_kind_open", brief)
        find = plan("SELECT " + k._CLAIM_COLS + " FROM claims WHERE inst=? AND attr IN ("
                    + k._FIND_ATTRS_SQL + ") AND ended_at IS NULL", ("demo",))
        self.assertIn("claims_find_v1", find)
        with self.store.reader() as con:
            via_index = sorted((r["kind"], r["key"], r["attr"])
                               for r in k.load_find_claims(con, "demo"))
            direct = sorted((r["kind"], r["key"], r["attr"])
                            for r in k.load_claims(con, "demo", attrs=k._FIND_ATTRS))
        self.assertEqual(via_index, direct)
        self.assertTrue(via_index)

    def test_set_and_withdraw(self):
        out, err = self.call("proj-set", {"ref": "3/1/7", "attr": "name", "value": "Lawn"})
        self.assertTrue(err)
        out = self.ok("proj-set", {"ref": "3/1/7", "attr": "name", "value": "Lawn",
                                   "said": "call it Lawn"})
        self.assertEqual((out["before"], out["now"], out["now_from"]), ("Garden light", "Lawn", "user"))
        out = self.ok("proj-set", {"ref": "3/1/7", "attr": "name", "value": None, "said": "undo"})
        self.assertEqual((out["now"], out["withdrawn"]), ("Garden light", 1))
        out, err = self.call("proj-set", {"ref": "3/1/7", "attr": "flags", "value": "x",
                                          "said": "s"})
        self.assertEqual(out["error"], "attr_not_settable")
        out = self.ok("proj-set", {"ref": "3/1/7", "attr": "dpt", "value": "DPST-1-1", "said": "s"})
        self.assertEqual(out["now"], "1.001")
        out = self.ok("proj-set", {"ref": "installation", "attr": "precedence",
                                   "value": ["esf"], "said": "trust the export"})
        self.assertEqual(self.ok("proj-group", {"ga": "3/1/7"})["name"], "Garden lamp")
        self.assertEqual(self.ok("proj-brief")["precedence"][0], "esf")

    def test_target_state_and_pending(self):
        self.ok("proj-set", {"ref": "1.2.3:P61", "attr": "value", "value": 4, "said": "4 s"})
        self.ok("proj-set", {"ref": "1.2.3#0>3/2/1", "attr": "present", "value": True,
                             "said": "link it"})
        self.ok("proj-set", {"ref": "1.2.30", "attr": "name", "value": "New actuator",
                             "said": "add one"})
        out = self.ok("proj-diff", {"a": "user", "b": "bus"})
        self.assertEqual(out["devices_only_in_user"]["items"], ["1.2.30"])
        self.assertEqual(out["links_only_in_user"]["items"], ["1.2.3#0>3/2/1"])
        p = out["params_to_change"]["items"][0]
        self.assertEqual((p["param"], p["want"], p["last_known"]),
                         ("1.2.3:P61", 4, {"value": 3, "source": "ets"}))
        self.assertEqual(self.ok("proj-brief")["pending_changes"], 3)
        self.ok("proj-set", {"ref": "1.2.3#2>3/1/8", "attr": "present", "value": False,
                             "said": "unlink"})
        out = self.ok("proj-diff", {"a": "user", "b": "bus"})
        self.assertEqual(out["links_to_remove"]["items"], ["1.2.3#2>3/1/8"])

    def test_notes(self):
        out, err = self.call("proj-note", {"kind": "rule", "text": "Ask first"})
        self.assertEqual(out["error"], "said_required")
        rule = self.ok("proj-note", {"kind": "rule", "text": "Never open the gate unasked",
                                     "about": ["3/2"], "said": "never open the gate"})["note"]
        f = self.ok("proj-note", {"kind": "finding", "text": "1.2.20 replaced 1.2.9?",
                                  "about": ["1.2.20", "1.2.9"]})["note"]
        self.assertEqual(f["status"], "open")
        out, err = self.call("proj-note", {"action": "update", "id": f["id"], "status": "confirmed"})
        self.assertEqual(out["error"], "said_required")
        self.ok("proj-note", {"action": "update", "id": f["id"], "status": "confirmed",
                              "said": "yes, swapped in 2019"})
        brief = self.ok("proj-brief")
        self.assertEqual([r["id"] for r in brief["rules"]], [rule["id"]])
        self.assertEqual(brief["open"]["findings"], 0)
        dev = self.ok("proj-device", {"address": "1.2.20"})
        self.assertEqual(dev["notes"][0]["status"], "confirmed")
        lst = self.ok("proj-note", {"action": "list", "kind": "rule"})
        self.assertEqual(len(lst["notes"]), 1)
        out, err = self.call("proj-note", {"text": "x", "about": ["nonsense"]})
        self.assertEqual(out["error"], "invalid_reference")

    def test_history_journal(self):
        self.ok("proj-set", {"ref": "3/1/7", "attr": "name", "value": "Lawn", "said": "s"})
        out = self.ok("proj-history")
        tools = [j["tool"] for j in out["journal"]]
        self.assertEqual(tools[0], "proj-set")
        self.assertIn("import:ets3", tools)
        self.assertEqual(out["journal"][0]["by"], "test")

    def test_installation_choice(self):
        k.create_installation(self.store, "second", "Second")
        out, err = self.call("proj-brief")
        self.assertEqual(out["error"], "choose_installation")
        self.assertEqual(self.ok("proj-brief", {"installation": "demo"})["installation"]["id"], "demo")
        out, err = self.call("proj-brief", {"installation": "nope"})
        self.assertEqual(out["error"], "unknown_installation")
        with self.assertRaises(k.StoreError):
            k.create_installation(self.store, "demo", "again")
        with self.assertRaises(k.StoreError):
            k.create_installation(self.store, "Bad Id", "x")

    def test_budget_and_pages(self):
        claims = [("param", "1.2.3:Q%d" % i, "name", "parameter %d %s" % (i, "x" * 80), None)
                  for i in range(400)]
        with self.store.writer() as con:
            k.put_claims(con, "demo", "ets", claims, "2015-01-01")
        out = self.ok("proj-device", {"address": "1.2.3", "section": "params"})
        self.assertIsNotNone(out["next_offset"])
        seen = len(out["params"])
        while out["next_offset"] is not None:
            out = self.ok("proj-device", {"address": "1.2.3", "section": "params",
                                          "offset": out["next_offset"]})
            seen += len(out["params"])
        self.assertEqual(seen, out["total"])


# ---------------------------------------------------------------------------
# MCP and HTTP
# ---------------------------------------------------------------------------

class TestMcp(Base):
    def rpc(self, method, params=None, mid=7):
        return k.handle_rpc({"jsonrpc": "2.0", "id": mid, "method": method,
                             "params": params or {}}, self.store)

    def test_initialize_and_lists(self):
        r = self.rpc("initialize", {"protocolVersion": "2025-03-26"})["result"]
        self.assertEqual(r["protocolVersion"], "2025-03-26")
        self.assertIn("proj-brief", r["instructions"])
        tools = self.rpc("tools/list")["result"]["tools"]
        self.assertEqual({t["name"] for t in tools}, set(k.TOOL_FUNCS))
        self.assertLess(len(json.dumps({"result": {"tools": tools}}).encode()), 12000)
        for t in tools:
            self.assertTrue(t["name"].startswith("proj-"))
            self.assertIn("readOnlyHint", t["annotations"])
        writers = {t["name"] for t in tools if not t["annotations"]["readOnlyHint"]}
        self.assertEqual(writers, {"proj-note", "proj-set", "proj-read-device"})
        open_world = {t["name"] for t in tools if t["annotations"].get("openWorldHint")}
        self.assertEqual(open_world, {"proj-read-device"})      # the only one with a bus

    def test_skill_resource(self):
        res = self.rpc("resources/list")["result"]["resources"]
        self.assertEqual(res[0]["uri"], k.SKILL_URI)
        text = self.rpc("resources/read", {"uri": k.SKILL_URI})["result"]["contents"][0]["text"]
        self.assertIn("proj-brief", text)
        self.assertEqual(self.rpc("resources/read", {"uri": "skill://x"})["error"]["code"], -32002)

    def test_errors(self):
        self.assertEqual(self.rpc("tools/call", {"name": "nope"})["error"]["code"], -32602)
        self.assertEqual(self.rpc("bogus")["error"]["code"], -32601)
        self.assertIsNone(k.handle_rpc({"jsonrpc": "2.0", "method": "notifications/x"},
                                       self.store))
        r = k.handle_rpc({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                          "params": {"name": "proj-set", "arguments": {
                              "ref": "3/1/7", "attr": "name", "value": "x", "said": "s",
                              "_by": "forged"}}}, self.store, caller="real")
        self.assertFalse(r["result"]["isError"])
        with self.store.reader() as con:
            by = con.execute("SELECT by FROM journal WHERE tool='proj-set'").fetchone()[0]
        self.assertEqual(by, "real")


class TestHttp(Base):
    def setUp(self):
        super().setUp()
        self.httpd = k.make_server("127.0.0.1", 0, self.data)
        self.httpd.log_line = lambda line: None
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        super().tearDown()

    def post(self, body, host=None):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        data = json.dumps(body).encode()
        c.request("POST", "/mcp", data, {"Content-Type": "application/json",
                                         "Host": host or "127.0.0.1:%d" % self.port})
        r = c.getresponse()
        out = (r.status, r.read())
        c.close()
        return out

    def test_mcp_post(self):
        st, body = self.post({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                              "params": {"name": "proj-brief"}})
        self.assertEqual(st, 200)
        res = json.loads(body)["result"]
        self.assertFalse(res["isError"])
        st, _ = self.post({"jsonrpc": "2.0", "method": "notifications/initialized"})
        self.assertEqual(st, 202)

    def test_host_guard_and_health(self):
        st, _ = self.post({"jsonrpc": "2.0", "id": 1, "method": "ping"}, host="evil.example")
        self.assertEqual(st, 403)
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        c.request("GET", "/healthz")
        r = c.getresponse()
        h = json.loads(r.read())
        c.close()
        self.assertTrue(h["ok"])
        self.assertEqual(h["installations"], 1)

    def test_refuses_any_address(self):
        with self.assertRaises(k.ConfigError):
            k.make_server("0.0.0.0", 0, self.data)


class TestCli(Base):
    def test_init_and_import(self):
        data = os.path.join(self.tmp, "cli")
        self.assertEqual(k.main(["init", "--data", data, "--installation", "x", "--name", "X"]), 0)
        buf = io.StringIO()
        old = sys.stdout
        sys.stdout = buf
        try:
            rc = k.main(["import", "ets3", self.dump, "--project", "Demo", "--installation", "x",
                         "--data", data])
        finally:
            sys.stdout = old
        self.assertEqual(rc, 0)
        self.assertIn("as of 2015-05-29", buf.getvalue())
        old_err = sys.stderr
        sys.stderr = io.StringIO()
        try:
            self.assertEqual(k.main(["import", "ets3", self.dump, "--installation", "x",
                                     "--data", data]), 2)
        finally:
            sys.stderr = old_err

    def test_backup(self):
        self.load_all()
        dest = os.path.join(self.tmp, "backup")
        os.makedirs(dest)
        for name in ("project-20000101-000000.sqlite.gz", "notes.txt"):
            self.path(os.path.join("backup", name), "old")
        r = k.backup_store(self.store, dest, keep_days=14,
                           now=datetime.datetime(2026, 10, 30, 3, 30))
        self.assertEqual(os.path.basename(r["path"]), "project-20261030-033000.sqlite.gz")
        self.assertEqual(r["removed"], ["project-20000101-000000.sqlite.gz"])
        self.assertEqual(sorted(os.listdir(dest)),
                         ["notes.txt", "project-20261030-033000.sqlite.gz"])
        copy = os.path.join(self.tmp, "copy.sqlite")
        with open(r["path"], "rb") as f, open(copy, "wb") as g:
            g.write(gzip.decompress(f.read()))
        con = k.sqlite3.connect(copy)
        try:
            n = con.execute("SELECT count(*) FROM claims").fetchone()[0]
        finally:
            con.close()
        with self.store.reader() as live:
            self.assertEqual(n, live.execute("SELECT count(*) FROM claims").fetchone()[0])
        self.assertFalse(os.path.exists(self.store.path + ".backup.tmp"))


# ---------------------------------------------------------------------------
# proj-read-device against a stand-in for the stick's MCP server
# ---------------------------------------------------------------------------

class FakeStick:
    """The part of a busware KNX stick's MCP surface proj-read-device uses."""

    def __init__(self):
        self.stick_id = "0123456789ab"
        self.serial = "FFF000000001"
        self.answers = True
        self.tables_ok = True
        self.stale = 0              # property reads that get the previous answer again
        self.last_prop = None
        self.assoc = [{"tsap": 1, "ga": "3/1/7", "com_obj": 0},
                      {"tsap": 3, "ga": "3/1/9", "com_obj": 5}]
        self.calls = []
        fake = self

        class Handler(k.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                msg = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                name = msg["params"]["name"]
                args = msg["params"]["arguments"]
                fake.calls.append(name)
                body = json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": {
                    "content": [{"type": "text", "text": json.dumps(fake.reply(name, args))}],
                    "isError": False}}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.httpd = k.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = "http://127.0.0.1:%d/mcp" % self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def reply(self, name, args):
        if name == "describe":
            return {"stick_id": self.stick_id,
                    "knx": {"own_address": "1.2.250", "tpuart_ready": True}}
        if name == "knx-describe":
            return ({"ok": True, "mask": "0x0701", "lsm": [{"idx": 1, "name": "Loaded"}]}
                    if self.answers else {"ok": False, "error": "not connected"})
        if name == "knx-prop-read":
            data = {13: "FFF0123410", 11: self.serial, 15: "FFFFFFFFFFFFFFFFFFFF"}[args["pid"]]
            if self.stale and self.last_prop:
                # a layer-2 repeat of the answer before, taken for this one
                self.stale -= 1
                return {"ok": True, "data": self.last_prop}
            self.last_prop = data
            return {"ok": True, "data": data}
        if name == "knx-tables-read":
            return ({"ok": True, "associations": self.assoc} if self.tables_ok
                    else {"ok": False, "error": "timeout"})
        return {"ok": False, "error": "unknown verb"}

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


class TestReadDevice(Base):
    def setUp(self):
        super().setUp()
        self.load_all()
        self.stick = FakeStick()
        os.environ["KNXPROJECT_STICK_URL"] = self.stick.url
        self.gaps = (k.MGMT_GAP_S, k.PROP_RETRY_S)
        k.MGMT_GAP_S = k.PROP_RETRY_S = 0

    def tearDown(self):
        k.MGMT_GAP_S, k.PROP_RETRY_S = self.gaps
        os.environ.pop("KNXPROJECT_STICK_URL", None)
        self.stick.close()
        super().tearDown()

    def call(self, name, args=None, mid=1):
        return super().call(name, dict(args or {}, _by="test"), mid)

    def read(self, address="1.2.3"):
        out = self.ok("proj-read-device", {"address": address, "said": "read it"})
        self.assertEqual(out["state"], "running")
        for _ in range(200):
            out = self.ok("proj-read-device", {"address": address})
            if out["state"] != "running":
                return out
            time.sleep(0.02)
        self.fail("the read did not finish")

    def bus_links(self):
        with self.store.reader() as con:
            return {r["key"] for r in k.load_claims(con, "demo", kind="link", keys="1.2.3#*",
                                                    source="bus")}

    def test_read_records_the_device_as_bus(self):
        self.assertIsNone(self.ok("proj-read-device", {"address": "1.2.3"})["state"])
        out, err = self.call("proj-read-device", {"address": "1.2.3", "said": "read it"})
        self.assertEqual(out["error"], "stick_unknown")
        self.ok("proj-set", {"ref": "installation", "attr": "stick_id",
                             "value": "01:23:45:67:89:AB", "said": "that stick"})
        self.assertEqual(self.ok("proj-brief")["stick_id"], "0123456789ab")
        out = self.read()
        self.assertEqual(out["state"], "done", out)
        self.assertEqual(out["links_new"], ["1.2.3#5>3/1/9"])
        self.assertEqual(out["links_gone"], ["1.2.3#2>3/1/8"])
        self.assertEqual(self.bus_links(), {"1.2.3#0>3/1/7", "1.2.3#5>3/1/9"})
        self.assertIsNone(out["identity"]["order_info"])            # all FF: unwritten
        self.assertEqual(self.stick.calls[:2], ["describe", "knx-describe"])
        tools = [j["tool"] for j in self.ok("proj-history")["journal"]]
        self.assertIn("proj-read-device", tools)
        self.assertIn("import:read-device", tools)

    def test_read_refuses_what_does_not_fit(self):
        self.ok("proj-set", {"ref": "installation", "attr": "stick_id",
                             "value": "0123456789ab", "said": "that stick"})
        before = self.bus_links()
        self.stick.stick_id = "ba9876543210"
        out = self.read()
        self.assertEqual(out["state"], "failed")
        self.assertIn("not this installation's stick", out["message"])
        self.stick.stick_id = "0123456789ab"
        self.stick.serial = "FFF000000099"
        out = self.read()
        self.assertEqual((out["state"], out["error"]), ("failed", "different_device"))
        self.stick.serial = "FFF000000001"
        self.stick.answers = False
        out = self.read()
        self.assertIn("did not answer", out["message"])
        self.assertEqual(self.bus_links(), before)
        self.stick.answers, self.stick.tables_ok = True, False
        out = self.read()
        self.assertEqual((out["state"], out["tables_error"]), ("done", "timeout"))
        self.assertEqual(self.bus_links(), before)                  # nothing ended

    def test_a_repeated_answer_is_not_taken_for_another_property(self):
        self.ok("proj-set", {"ref": "installation", "attr": "stick_id",
                             "value": "0123456789ab", "said": "that stick"})
        self.stick.stale = 1                       # serial read gets the ApplicationId
        out = self.read()
        self.assertEqual((out["state"], out["identity"]["serial"]), ("done", "FFF000000001"))
        self.assertNotIn("unreadable", out)
        self.stick.stale, self.stick.last_prop = 4, None   # serial and order info, twice each
        out = self.read()
        self.assertEqual(out["state"], "done", out)
        self.assertEqual(out["unreadable"], ["order_info", "serial_number"])
        with self.store.reader() as con:
            serial = k.load_claims(con, "demo", kind="device", keys=["1.2.3"],
                                   attrs=["serial"], source="bus")
        self.assertEqual([r["value"] for r in serial], ["FFF000000001"])   # not ended

    def test_mdns_resolve_gives_up(self):
        t0 = time.monotonic()
        self.assertIsNone(k.mdns_resolve("no-such-stick-here", timeout=0.3))
        self.assertLess(time.monotonic() - t0, 2.0)


if __name__ == "__main__":
    unittest.main()
