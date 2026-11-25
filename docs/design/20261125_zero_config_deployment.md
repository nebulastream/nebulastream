# General Information

This design document proposes a zero-config deployment of NebulaStream: a worker installs from a package, finds the coordinator, describes itself, and joins after the operator admits it; every parameter has a safe default.
It contributes to the epic #2448 (areas `INSTALL`, `DEPLOY`, `CONFIG`, `RELEASE`) and builds on the standalone coordinator server of #2245.
Lessons learned from a prototype of the proposed mechanism are at the end.

# The Problem

Today a worker is started with explicit addresses (`--grpc`, `--data_address`).
A person registers it with `CREATE WORKER 'host:8080' SET ('host:9090' AS DATA)` in `nes-repl` or lists it in a topology file for `nes-cli`.
Every source and sink is bound with `HOST`.
The coordinator is a library that each frontend starts itself (#2245), so no long-running component holds the workers.
Images are built locally (tag `local`), and the repository publishes no release.

- **P1.** A new worker is invisible until registered by hand.
- **P2.** The address is the only identity; a changed IP or replaced device breaks the topology.
- **P3.** Nothing decides which machines may join.
- **P4.** No installable artifact for an edge node: no release, no package, no service unit, no documented arm64 image.
- **P5.** Each frontend has its own catalog, so two frontends cannot work on one deployment, and no catalog outlives its frontend (#2245).
- **P6.** No tested path from an empty machine to a first query.
- **P7.** Many parameters must be chosen by hand; nothing suggests good values.
- **P8.** Node properties (cores, memory, architecture, devices, version) are typed in, although the machine knows them.

# Goals

- **G1.** An installed node with a one-line coordinator hint joins without further input (P1).
- **G2.** Stable identity independent of the IP; defined handling of replaced or re-addressed nodes (P2).
- **G3.** The operator decides who joins; unknown machines are not admitted by default (P3).
- **G4.** Versioned artifacts for x86-64 and arm64: images, a package with service unit, a compose file for the coordinator host (P4).
- **G5.** One catalog per deployment that outlives the frontends (P5).
- **G6.** The path from empty machine to first query is documented and tested in CI (P6).
- **G7.** Manual registration and topology files keep working.
- **G8. Good defaults.** Every parameter has a safe default that works on the smallest supported device (1-4 cores, 1-4 GB) without tuning.
  Where no safe default exists, the start fails and names the parameter (P7).
- **G9. The worker describes itself.** The worker gathers all information the coordinator needs and sends it with the enrollment.
  The operator supplies only a name wish, labels and the admission decision (P8).

# Non-Goals

- **NG1.** Starting or placing worker processes (that is the deployment layer: systemd, Podman, Kubernetes).
- **NG2.** OTA updates, image building, device certificates.
  NebulaStream reports its version only.
- **NG3.** Nodes that cannot reach the coordinator directly (NAT, relays).
  Not built; not made impossible.
- **NG4.** Coordinator high availability, mutual TLS, per-user authorization.
- **NG5.** Placing sources by discovered devices (devices are reported; using them is later work).
- **NG6.** Helm chart, DaemonSet, Kubernetes operator before a site asks.
- **NG7.** Tuning parameters while queries run; defaults are derived once at start.

# Alternatives

| | Approach | Why not (alone) |
|---|---|---|
| A1 | mDNS / DNS-SD discovery, no coordinator | multicast is often blocked; no admission, names or state |
| A2 | Generated static topology (Ansible, Helm values) | not zero-config; does not follow a changed address |
| **A3** | **Workers enroll with a long-running coordinator** | **proposed**; covers P1-P5, P8; adds a component to operate |
| A4 | Gossip / peer-to-peer membership | large new surface for a one-root tree |
| A5 | Persistent controllers only | solves P5 for one operator; nothing else |

A3 in stages, each stage useful alone.
A2 stays for fleets that already generate config; A1 may later feed the worker's `coordinator` setting.
G8 and G9 are independent of the alternative.

# Related Work

- #2448 (epic Usability): goals `CONFIG` (near-zero configuration by sensible defaults), `DEPLOY` (a first query in a few steps), `INSTALL`, `RELEASE`.
  This design document is a concrete plan for these four areas.
  The epic puts security out of scope; admission and the admin token of this proposal are therefore tracked here.
- #2244 (epic Coordinator Extensions & Hardening) and #2245 (standalone `nes-server` shared by frontends): WP4 below is delivered by #2245.
- #2371 (design document: namespaces and sessions in the coordinator): enrollment and admission must fit its model.

# Our Proposed Solution

```
 edge node                               coordinator host
 +--------------------+                  +-------------------------------------+
 | nes-worker         |  POST /enroll    | coordinator service                 |
 |  collects its own  | ---------------> |  enroll listener (workers connect)  |
 |  facts, clock,     |  facts, version  |  admin API (token, behind a proxy)  |
 |  link measurements | <--------------- |  persistent catalog                 |
 |  worker.yaml:      |  pending/admitted|  admission: approval|token|open     |
 |  coordinator hint  |                  |  owns name, labels, capacity        |
 +--------------------+                  +-------------------------------------+
                                                       ^ nes-repl / nes-cli (names, not addresses)
```

devOp flow: (1) `docker compose up` on the coordinator host; (2) install the worker package with `coordinator:` in `worker.yaml`, it appears as `pending`; (3) `nes-cli admit <name>` (or a pre-baked enroll token), then `HOST 'press-3'` and queries as usual.

## The worker describes itself (G9)

> **The worker gathers all information the coordinator needs on its own and sends it with the enrollment. The operator never types a value the machine can measure.**

| Fact | Gathered from | Used for |
|---|---|---|
| Node id, hostname | hostname + machine id (explicit name overrides) | identity, default name |
| Platform, version | architecture, OS, kernel, build | skew warning, compatibility |
| CPU | cores available to the process (cgroup-aware), instruction sets | threads, capacity |
| Memory | total and container/service limit | memory budget, capacity |
| Storage | free space of the data directory | file connectors |
| Ports | the ports the worker bound | informational (coordinator records the address it sees) |
| Capabilities | built-in sources, sinks, functions, inference runtimes, execution modes | reject unrunnable plans before deployment |
| Devices | serial, GPIO, USB, cameras | binding sources, later placement |
| Clock | offset to the coordinator, measured at enrollment | stream alignment, skew warning |
| Links | round-trip time and throughput to coordinator and parent | link description for placement, no hand-set bandwidth |
| Load | free slots and memory, with the heartbeat | placement, rebalancing |

- Prototype reported: identity, architecture, cores, memory, version, ports (devices: empty slot).
  The rest is new and must be validated on devices.
- Facts are claims, not credentials: they inform placement; they never decide admission.
- The worker owns the facts; the coordinator owns name, labels and capacity (`max_operators`).
- Facts are re-sent on change, logged at start, and shown by `SHOW WORKERS`.

## Good defaults (G8)

Rules: every parameter has a default; derive from the machine before fixing a constant; safe over convenient (unsafe is an explicit opt-in with a warning); print the effective configuration at start with the source of each value (default, derived, file, flag, environment); fail with a recipe when no safe default exists; test the defaults.

Starting points (initial proposals; numbers to be set by measurement on a 1 GB and a 4 GB device):

| Parameter | Default | Source |
|---|---|---|
| Worker gRPC / data address | all interfaces, standard ports | fixed |
| Node name | hostname, else `node-<id>` | derived |
| Coordinator | `auto` in the package; unset in embedded and manual use | package |
| Threads | cores available to the process | derived |
| Memory budget | share of memory available to the process, with a system reserve | derived |
| Capacity (slots) | from cores and memory; coordinator may override | derived |
| Allowed file paths | package data directory only | package |
| Allowed endpoints | none; sites add them; credentials never go to unlisted endpoints | safe |
| Coordinator admin listener | loopback | safe |
| Coordinator enroll listener | all interfaces, one port, enrollment and health only | fixed |
| Coordinator state | platform state directory, created at first start | derived |
| Admin token | generated at first start, owner-only file, never printed | generated |
| Admission | `approval` | safe |
| Pending enrollments | 100 total, 5 per source address, expire after 24 h | fixed |
| Root worker | worker on the coordinator host, created automatically | derived |
| Topology | enrolled workers below the root; link properties measured | derived |
| Version skew | warn, do not refuse | fixed |

## Work packages

Sizes: S days, M weeks, L a month or more.

- **WP1 Versioned artifacts (M).** Versioned images (`vX.Y.Z`) incl. arm64; native arm64 build image and CI leg; worker `.deb` with systemd unit (restart, memory/CPU limits), Quadlet file and baked `/etc/nes/worker.yaml`; run the release workflow in a scratch fork.
  *Accept:* clean Debian, Ubuntu, Raspberry Pi OS install starts the worker; restart works; idle and peak memory recorded on a Pi.
- **WP2 Self-description (M).** Stable node id; one collector per fact group (a failing group yields "none", never stops the worker); clock and link measurement; names accepted wherever an address is (`HOST 'press-3'`).
  *Accept:* collectors tested against `/proc` and `/sys` fixtures; facts equal `nproc`/`free`/`lscpu`, also under container limits; `nes-repl` shows facts of a manually added worker.
- **WP3 Defaults (M).** One configuration layer (flag, file, environment, derived, default); effective-configuration log; recipe errors; memory share, capacity and thread count set by measurement.
  *Accept:* every executable starts with no options on a 1-core 1 GB container and a large machine; derived values in range; every parameter's default and derivation is in the reference docs.
- **WP4 Coordinator service (L, tracked in #2245).** This design document adds only what zero-config needs from it: a persistent catalog, an admin REST API with bearer token, refusal to start on a non-loopback bind without token and an acknowledgement that TLS is terminated in front (or an explicit insecure flag), and a token generated at first start. `nes-repl` and `nes-cli` keep working without it.
  *Accept:* restart keeps admitted workers and does not dial pending ones; auth refusals; manual path unchanged.
- **WP5 Enrollment and admission (M).** `POST /v1/enroll` on its own listener; modes `approval` (default), `token`, `open`; pending limits; coordinator records the peer address; versioned protocol; facts in the body; worker registrar (`coordinator: host:port | auto`, back-off, inert when unset); clone and move rules (see lessons).
  *Accept:* compose test with two enrolling workers and no `CREATE WORKER`; all three modes; clone and move cases; abuse limits; an unreachable coordinator never stops the worker.
- **WP6 Controllers by name (S).** `nes-repl` / `nes-cli` list pending workers, admit, rename, label; default topology below the root; link properties from measurements with manual override.
  *Accept:* a query from `HOST 'a'` to `HOST 'root'` runs with no address typed.
- **WP7 Site install and hardening (M).** `install.sh`; site install guide; security page; reviewed package defaults; site acceptance script run on one real line.
  *Accept:* someone who has not seen the code follows the guide on two machines.
- **WP8 Later.** Helm/DaemonSet, relay, mDNS, version enforcement, device-based placement, reuse of prototype code (enrollment crate, registrar).

Goal coverage: G1 WP2, WP5 · G2 WP2, WP5 · G3 WP5 · G4 WP1 · G5 WP4 · G6 CI tests of WP1, WP5, WP7 · G7 manual path kept, enrollment off unless configured · G8 WP3 · G9 WP2, WP5.

## Verification (end-to-end suite)

1. Coordinator, root worker and two workers, no `CREATE WORKER`: both listed as pending with their facts; after admit a query across them returns data.
2. `open` admits at once and warns; `approval` leaves the node pending and the coordinator makes no connection; `token`: wrong token refused, right token admits.
3. Re-enrolling is idempotent; same id from a second address while the first is live is a conflict; changed hostname is a new node; after removal the id enrolls from the new address; the name override gives the same id.
4. Non-loopback bind without token and acknowledgement refuses to start; 401/200 as expected; health and enrollment stay open; generated token is owner-only and in no log.
5. Name and address resolve to the same worker; unknown name and pending name give distinct errors.
6. Sixth pending enrollment from one address refused; pending row expires; squatted id cleared by deleting it.
7. After a coordinator restart admitted workers are active again; pending ones are still not dialed.
8. A worker reporting no or a malformed version enrolls and shows as unknown.
9. Install on clean arm64 and x86-64; service start and restart; baked config enrolls on first boot.
10. Data plane works between two separate hosts with the packaged defaults.
11. Defaults: every executable starts with no options on 1 core / 1 GB and on a large machine; effective configuration printed; first query runs; a missing mandatory value fails with a message naming it.
12. Reported cores, memory, architecture equal the machine's (also under limits); no device subsystem gives an empty list; clock offset and link round trip are within tolerance of a known setup.
13. All existing tests that use manual registration pass unchanged.

# Proof of Concept

The prototype passed checks 1-9 in a lab and on a three-tier demo cluster.
Open: arm64 binaries, the release workflow, systemd on bare metal, the data plane between separate machines, reviewed package defaults, and the new self-description parts (capabilities, clock, links).
First proof for the public repository: WP1 + WP2 + WP3, which need no coordinator: a package, a stable name, facts in the REPL, a worker that starts on a Pi without options.

# Summary

The plan removes manual registration (P1, P2, P8), puts admission in front of the control plane (P3), makes nodes installable and releases repeatable (P4), keeps the catalog (P5), tests the whole path (P6) and removes parameter guesswork (P7), while manual registration and the embedded REPL stay.
A3 is the only alternative that solves P1-P5 together.
Risks: the arm64 build, the coordinator as a new component to operate, and the defaults of a deny-by-default worker.

# Open Questions

- Is the enroll listener part of the `nes-server` of #2245 or a second binary?
- How do enrollment and admission fit the namespaces and sessions of #2371?
- Where is security (admission, tokens) tracked, since #2448 excludes it?
- Which defaults are acceptable in a public package: endpoint allow-list, data-port bind address, `approval`?
- Memory share on a 1 GB device and the capacity formula: needs measurement.
- Which reported facts (device lists, interface addresses) may be exposed; can the operator hide some?
- How to measure link throughput without disturbing queries (probe at enrollment, then passive estimates)?
- Where does the arm64 build run, and who owns it?
- Registry, names, release cadence and tag scheme.
- Does `auto` discovery ship in the first release?
- Which state formats of the controllers (CLI query state backend) must stay readable?

# Sources and Further Reading

- Public README: Quick Start, "Start the client with a worker".
- `docs/development/packaging_docker_images.md`; `docs/guide/nebulastream-frontend.md`.
- Epic #2448 (Usability), epic #2244 and feature #2245 (coordinator), design document PR #2371 (sessions and namespaces).

# Lessons Learned from the Prototype

1. **Identity is not the address.** Node id from hostname + machine id (an explicit name overrides); an address in the id creates duplicates.
   In containers fix the hostname.
2. **Record the address the coordinator sees, not the claimed one.** Hence enrollment cannot pass through a proxy or NAT: it needs its own listener on the factory network; the admin API may sit behind a TLS proxy.
3. **Default to approval.** `open` is an explicit opt-in with a warning; a pre-shared token admits a pre-authorised fleet.
4. **Bound the unauthenticated surface:** cap pending enrollments (total, per source), expire them, serve only enrollment and health.
5. **Define clones and replacements.** Same id from a second address while the first node is live: conflict (cloned image).
   A node returning from a new address after its old row is gone: moved with name, labels, sources and sinks; the old row's running work fails (not migrated).
   Recovery took about five minutes with back-off.
6. **Zero-config collides with deny-by-default.** Empty allow-lists reject every file and network connector.
   Ship a safe data root; decide endpoint defaults explicitly; never send credentials to an unlisted endpoint.
7. **No coordinator by default.** A worker without a configured coordinator enrolls with nobody; the package turns it on.
8. **Enrollment failure never stops a worker.** Back off, log how to configure at most once a minute, end waits on SIGTERM.
9. **Kubernetes workers need host networking;** a pod IP changes on restart and so would the recorded address.
10. **Version skew is normal during a rollout.** Report the version, show the mismatch, refuse nothing in v1.
11. **Packaging fails in its own ways.** Binaries need the distribution's glibc and libc++; arm64 needs a native runner or its own build image; a release workflow that never ran is not a process; test systemd and published images on real hosts.
12. **Verify end to end and date the docs.** The checks caught most defects; docs went stale within weeks.
13. **Keep the manual path.** Enrollment writes the same catalog rows as `CREATE WORKER`.
14. **The worker owns the facts, the coordinator owns the decisions.** Mixing them let a typed value override a measured one.
