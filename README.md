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

It is one of two services that run next to **MCP4KNX**, the firmware that lets
an AI agent commission KNX devices without ETS through a busware TUL stick: the
stick finds this service on the LAN and forwards its tools to the agent. The
other service, [knxprod-mcp](https://github.com/tostmann/knxprod-mcp), supplies
the manufacturers' product data. Firmware and web flasher:
[install.busware.de/TUL/mcp/](https://install.busware.de/TUL/mcp/);
documentation: [BREMA for KNX](https://github.com/tostmann/BREMA/blob/main/docs/brema-for-knx.md).

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

## Quick start

1. **Start the container.** With the `docker-compose.yml` from this repository
   (it uses the published image `tostmann/knxproject-mcp`, for linux/amd64 and
   linux/arm64):

   ```sh
   docker compose up -d
   docker compose logs                 # "created installation 'home' ...", "listening on ..."
   ```

   At the first start the server creates the installation `home`
   (`KNXPROJECT_INSTALLATION`, its name in `KNXPROJECT_INSTALLATION_NAME`). A
   record that already holds an installation is never changed by these two.
   The record lives in the Docker volume `knxproject-data`; keep it on a local
   disk (SQLite locking).

   The Compose file uses host networking (Linux). The server then listens on
   the host's LAN address only (`KNXPROJECT_BIND=auto`), port 8096, and
   announces itself via mDNS/DNS-SD as `_knxproject-mcp._tcp`. On Docker
   Desktop, switch to port mapping as described in `docker-compose.yml`.

2. **Connect.** A busware KNX stick on the same network finds the server by
   itself and offers its `proj-*` tools to its own clients, through its remote
   access as well. Any other MCP client connects to `http://<host>:8096/mcp`
   (Streamable HTTP, JSON responses), for example Claude Code:

   ```sh
   claude mcp add --transport http knxproject http://<host>:8096/mcp
   ```

3. **Name the stick.** `proj-read-device` and `stick-check` reach the
   installation's stick by its name `busware-knx-<stick_id>.local`. The agent
   records the id (the stick's `describe` shows it) with `proj-set`
   `ref=installation attr=stick_id`. A stick with an admin password needs it in
   `KNXPROJECT_STICK_TOKEN`. Without host networking, give the stick's URL in
   `KNXPROJECT_STICK_URL`.

Without Compose:

```sh
docker run -d --name knxproject-mcp --network host --restart unless-stopped \
  -e KNXPROJECT_BIND=auto -e KNXPROJECT_INSTALLATION=home \
  -v knxproject-data:/data -v "$PWD/import:/import:ro" \
  tostmann/knxproject-mcp
```

The commands below run inside the container, e.g.
`docker compose exec knxproject-mcp python /app/knxproject_mcp.py init
--installation flat --name "Flat"` for a second installation. With more than
one, the tools need the argument `installation`.

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
# in the container, with the file in ./import:
docker compose exec knxproject-mcp python /app/knxproject_mcp.py import esf /import/house.esf --installation home
```

| format | source | input |
|---|---|---|
| `ets3` | `ets` | an ETS3 project database dumped to one gzip CSV per table (`--project NAME`) |
| `knxproj` | `knxproj` | a project archive exported by ETS 5.7 or later (from ETS 6 also with a project password) |
| `esf` | `esf` | ETS group address export |
| `inventory` | `bus` | the reply of a busware KNX stick's `knx_inventory` |
| `observe` | `observed` | `knx_observe_list` (traffic per group address or sender) |
| `ha-yaml` | `ha` | the `knx:` block of a Home Assistant configuration |
| `fhem` | `fhem` | `fhem.cfg` files (EIB/KNX devices); dated backups give a time line |
| `claims` | `--source` | JSON lines `{"ref": "1.2.3#4", "attr": "name", "value": "..."}` |

From a `.knxproj` the import reads the topology, the devices with the product
data the archive carries (product, order number, application, ApplicationId and
serial number as a stick reads them), locations, group addresses with their
datapoint types, the objects a device shows or links, the links (the first
group address of an object is its sending one; ETS moves the address made
sending to the front), names, descriptions and comments (a text ETS wrote as
RTF is reduced to its text; what ETS 6.4 shows as a device's comment it
exports as `InstallationHints`) and the parameter values the project lists.
In the ETS 6.4 exports checked, a project lists only the parameters whose
value differs from the default; one at its default gets no claim.

Objects and parameters in module instances are read with the values ETS
stored for the instance's arguments: an object's number counts from the
argument its `BaseNumber` names, a parameter's memory from the one its
`BaseOffset` names, and texts get their `{{…}}` placeholders filled in as ETS
shows them. A parameter in a module is `device:<instance>_<parameter>`, e.g.
`1.1.2:MD-1_M-21_MI-1_UP-2`. Modules inside modules are not read; the import
counts them, and whatever else it passed over, under `archive` in its stats,
and the command prints that line.

ETS 6 encrypts a project that has a project password (in the export checked,
ETS 6.4 required one for a project with a KNX Data Secure device). The import
decrypts it with the password from `KNXPROJ_PASSWORD`, or asks for it at a
terminal; the password is used for that import only and stored nowhere, and
the security keys the project holds are not read. A password-protected archive
from ETS 5 is refused: export it without a password.

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
tostmann/knxproject-mcp backup /backup`.

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
