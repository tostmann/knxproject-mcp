# knxproject-mcp — the record of a KNX installation

This server remembers one or more KNX installations for you: topology, devices,
group addresses, com objects, links, parameters, and the notes, findings and
rules of the house. You have no other memory of the installation between
conversations. Treat this record as the place where knowledge about the
installation lives, and keep it honest.

## Start of every conversation

1. Call `proj-brief`. Read the description and the **rules**. Rules are set by
   the operator and bind you (for example "never switch the door openers
   without asking").
2. Look at `sources` (what was imported, and as of when), `open` (findings and
   todos nobody has settled yet) and `pending_changes`.

## How the record is built

Every value is a **claim**: entry + attribute + value + source + dates. Several
sources can say different things about the same entry. Nothing is deleted:
when a source stops saying something, its claim ends and stays in the history.

| source | what it means |
|---|---|
| `user` | what the operator decided or told you (written with `proj-set`, quoting them) |
| `bus` | read from the device itself (address and association tables, application id) |
| `observed` | heard on the bus (traffic on a group address, a device that sends) |
| `ha` | the Home Assistant configuration that operates the installation |
| `fhem` | FHEM configuration backups (older operation; `fhem_autocreate` = the address sent traffic but nobody named it) |
| `ets` | an ETS project database, possibly years old (see `as_of`) |
| `knxproj`, `esf` | ETS exports |
| `product` | manufacturer product data (names of com object *slots*, not functions) |

The **precedence** in `proj-brief` decides which value counts (default: user,
bus, observed, ha, fhem, ets, knxproj, esf, product). `name_differs` /
`differs` tell you that other sources disagree — then show the operator both
and say where each comes from. Do not pick a winner by taste.

## References

`1.2.3` device · `1.2` line · `area:1` · `3/1/7` group address ·
`3/1` middle group · `range:3` main group · `1.2.3#24` com object 24 ·
`1.2.3#24>3/1/7` link · `1.2.3:P61` parameter · `installation`.

## Typical tasks

**What is X / where is X?** `proj-find` (words or an address), then
`proj-group` for a group address (all names by source, linked devices and
objects, Home Assistant entities, traffic) or `proj-device` (summary,
`section=objects`, `section=params` with `query`).

**Draw the installation.** `proj-topology format=mermaid`. `view=physical`:
areas, lines, devices; dashed devices are known only from a project file and
were not confirmed on the bus. `view=functional range=3/1`: group addresses
and the devices linked to them. Render the Mermaid text as a diagram.

**Compare the project with reality.** `proj-diff` (default `a=ets b=bus`):
devices, application ids and links one source has and the other has not. A
difference is a lead, not a verdict: the project may be old, a device may have
been readdressed or replaced, the bus read may have missed a device. Write what
you conclude as a finding, with the evidence.

**Record what the operator says.** Names, descriptions, locations, datapoint
types: `proj-set` with `said` = their words. A standing instruction:
`proj-note kind=rule said=...`. Something to do later: `kind=todo`.

**Record what you inferred.** `proj-note kind=finding`, with `about` and the
evidence in the text. A finding stays `open` until the operator confirms or
rejects it (`action=update status=confirmed said=...`) or a bus read settles
it. Never write an inference with `proj-set`.

**Plan a change** (a new actuator, a link, a parameter value). Record the
target state with `proj-set` once the operator asked for it:
`1.2.12 present=true` plus product/app_id, `1.2.3#24>3/2/20 present=true`,
`1.2.3:P61 value=4`. `proj-diff a=user b=bus` is then the list of work
still to be done on the devices. This server never touches the bus; the
devices are programmed with the KNX stick's own tools (they default to a dry
run), only after the operator agreed. After programming, the devices are read
again and imported as `bus`, and the difference disappears.

**Bring the record up to date with a device.** `proj-read-device` reads one
device through the installation's stick (`stick_id` in `proj-brief`) and
records identity, load state and both tables as `bus`. It only reads, but it
is traffic on a working bus: call it only when the operator asked, with
`said` = their words. It runs in the background; call it again with the
address only for the result. `links_new` / `links_gone` say what changed
since the last bus read. A device that answers with a different serial
number is reported and not recorded. Do not copy a reply of the stick's own
read tools into the record by hand; this tool exists so that data never
passes through you.

**Product knowledge.** `proj-device` gives the `app_id`. If `prod-*` tools are
available (knxprod-mcp), they explain com objects and parameters of that
application program.

## Pitfalls

- A name from one source is not proof of what a channel does today. When
  sources disagree, say so.
- `ets` data can be many years old; check `as_of` in `proj-brief` and the dates
  in `claims` before relying on it. `as_of: null` with a `file_date` means
  nobody knows when the data was true; the file date only says when the file
  was last written.
- The KNX stick you can reach is wired to one bus, and that need not be this
  installation's. Before a stick tool reads or writes a device listed here,
  compare the stick's describe (stick id, own address) with `bus_access` in
  `proj-brief`, and look for this installation's senders in the stick's
  passive observe list. If they do not match, say so instead of acting.
- A parameter with `ets_only: true` exists only in the ETS project — the device
  does not store it, so it cannot be read back from the device.
- `slot_name` (from `product` data) labels the slot of the application
  program; in another channel mode the same number may do something else.
  Prefer a `name` from `ets` or the operator; check `channel_modes`.
- Old BCU1 devices may number objects differently in the project and in the
  device tables.
- `proj-set` and `proj-note` change this record only. They never switch,
  program or reset anything.
- The record describes somebody's building. Do not copy its contents anywhere
  the operator did not ask for.

## Limits

Replies stay under about 12 KB. Lists are paginated: pass `offset` =
`next_offset` until it is null. Narrow big answers with `query`, `range` or
`section`.
