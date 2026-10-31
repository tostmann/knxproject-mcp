# knxproject-mcp

An MCP server that keeps the record of a KNX installation for an AI agent:
topology, devices, group addresses, com objects, links, parameters, and the
notes, findings and rules of the house. An agent without a file system (a
chat assistant) forgets an installation between conversations; this server is
its memory. Python standard library only, one file, SQLite.

Every statement is a **claim** with its source and dates. Several sources may
say different things about the same group address (an old ETS project, the
Home Assistant configuration, what the device itself holds); the installation's
precedence decides which value counts, the others stay queryable. Claims are
never deleted: an import that no longer states one ends it.

The writing tools record what the operator decided and what the agent
inferred; they never talk to a KNX bus. One tool does: `proj-read-device`
reads a device through the installation's busware KNX stick (found by mDNS)
and records what it holds. It only reads, and only when asked to.

## Tools

| tool | |
|---|---|
| `proj-brief` | start here: description, rules, sources, open findings, pending changes |
| `proj-find` | search names, descriptions, functions, parameter texts, notes |
| `proj-group` | one group address with every source's claim, or a range |
| `proj-device` | one device: summary, com objects, parameters |
| `proj-topology` | physical or functional structure, optionally as a Mermaid flowchart |
| `proj-diff` | compare two sources (e.g. ETS project vs. bus; operator's target vs. bus) |
| `proj-history` | every claim ever made about an entry; the journal |
| `proj-note` | notes, findings (open until confirmed), rules, todos |
| `proj-set` | record a decision of the operator (names, target state, the stick) |
| `proj-read-device` | read one device through the installation's stick and record it as `bus` |

The skill `skill://knxproject-mcp/SKILL.md` explains the workflow to the agent.

## Run

```sh
docker build -t knxproject-mcp .
docker run -d --name knxproject-mcp --network host \
  -e KNXPROJECT_BIND=auto -v knxproject-data:/data -v "$PWD/import:/import:ro" \
  knxproject-mcp
docker exec knxproject-mcp python /app/knxproject_mcp.py init --installation home --name "My house"
```

The server listens on port 8096 (`/mcp`, Streamable HTTP, JSON responses) and
announces itself via mDNS/DNS-SD as `_knxproject-mcp._tcp`. Keep the data
directory on a local disk (SQLite locking).

## Product data

A record does not need the product data's descriptions of its parameters: a
parameter's value and where it lives in the device's memory are enough, when
the server is given a product pack written by
[knxprod-mcp](https://github.com/tostmann/knxprod-mcp) (`pack`):

```sh
knxproject_mcp.py serve --product-pack /data/products.pack   # or KNXPROJECT_PRODUCT_PACK
```

`proj-device section=params` then names the parameters that have no name of
their own in the record and says what their values mean, found by memory
location in the program of the device's application id. Where several
parameters share a location and mean different things (union members,
parameters of another channel mode) the answer lists them as `candidates` with
the conditions under which each applies. Where they mean the same, the answer
gives that meaning with the conditions under which the product data has it
there at all (`when`, `when_any`): in another channel mode the location may
hold a parameter the program does not describe there. A source of the record
(an ETS project) always wins over the product data.

The texts are English. With `proj-set ref=installation attr=language
value=de` they are German where the pack has German (knxprod-mcp keeps it
beside the English for names, blocks, value texts and conditions).

## Import

Bulk data is imported with commands, never through the agent:

```sh
knxproject_mcp.py import FORMAT PATH... --installation ID [--as-of DATE]
```

| format | source | input |
|---|---|---|
| `ets3` | `ets` | an ETS3 project database dumped to one gzip CSV per table (`--project NAME`) |
| `esf` | `esf` | ETS group address export |
| `inventory` | `bus` | the reply of a busware KNX stick's `knx_inventory` |
| `observe` | `observed` | `knx_observe_list` (traffic per group address or sender) |
| `ha-yaml` | `ha` | the `knx:` block of a Home Assistant configuration |
| `fhem` | `fhem` | `fhem.cfg` files (EIB/KNX devices); dated backups give a time line |
| `claims` | `--source` | JSON lines `{"ref": "1.2.3#4", "attr": "name", "value": "..."}` |

An import speaks for its source as of its date. One older than what the record
already holds from that source (a backup imported after a newer one) extends
the history and ends nothing; what only the older file states arrives ended,
since the newer import did not state it. The import's stats say so
(`record_newer`, `arrived_ended`, `stale_units`). Older is decided per unit:
per device for bus scans and reads -- a scan also records the address ranges
it searched (`line … scanned`), so a later scan that found nothing at an
address counts as newer data for it -- per statement for claims files, and for
the whole source otherwise.

## Backup

```sh
knxproject_mcp.py backup DEST [--keep-days 14]
```

writes a consistent, gzipped copy of the database into `DEST` (read back and
checked) and removes copies older than `--keep-days`. Run it daily, e.g. from a
systemd timer with `docker run --rm -v knxproject-data:/data -v /backups:/backup
knxproject-mcp backup /backup`.

`knxproject_mcp.py stick-check [--installation ID]` finds the installation's
stick and checks that it answers, without bus traffic.

## A record for a stick

```sh
knxproject_mcp.py export DEST [--installation ID] [--product-pack PACK]
```

writes one installation's record as a file a busware KNX stick with a record of
its own loads (upload on its landing page): only that installation, a rollback
journal instead of WAL, written atomically and read back. With
`--product-pack` it leaves out the parameter names, paths and value texts that
pack says again -- most of a record's rows -- so the stick needs the same pack.
They stay where the pack names a parameter otherwise than the record does (in
English or German): at such a place the product data may hold another
parameter, one of another channel mode, say. They also stay where the pack
gives the parameters at that place one name in English but several in German,
or the other way round. Parameters of applications the pack lacks keep their
texts. The command lists per application how many parameters the pack names.

On such a record `proj-find` does not find parameters by name;
`proj-device section=params query=…` does. The texts from the pack come in the
installation's language (above), those the record kept in the record's.

## Test

```sh
python3 tests/test_knxproject_mcp.py
```

## License

MIT. The data you import stays yours; nothing leaves the server unless a
client asks for it.
