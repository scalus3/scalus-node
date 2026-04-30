# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & test

Scala 3.3.7 multi-module sbt build. Most modules cross-build JVM + Scala.js (`crossProject`); native-dependent modules are JVM-only (see below).

Two convenience aggregates exist on top of the per-module projects:
- `jvm` — every JVM-published module + JVM-side of cross modules.
- `js`  — every JS-side of cross modules (no native deps).

```
sbt jvm/compile                     # compile the `jvm` aggregate (published JVM modules)
sbt jvm/test                        # run tests for the `jvm` aggregate (published JVM modules)
sbt 'scalus-cardano-network-it/test' # run the `scalus-cardano-network-it` integration-test suite explicitly
sbt js/compile                      # compile the `js` aggregate
sbt scalafmtAll scalafmtSbt         # format sources + build.sbt
sbt compileAll                      # alias: format + compile (Test) + testQuick
sbt testAll                         # alias: clean + format + compile + full test
```

Run a single suite or test:

```
sbt 'scalusStreamingCoreJVM/testOnly scalus.cardano.node.stream.engine.EngineSpec'
sbt 'scalusStreamingCoreJVM/testOnly *EngineSpec -- -z "rollback past k"'
```

Snapshots of `org.scalus` artifacts (the upstream `scalus-cardano-ledger`, etc.) come from Sonatype Central Snapshots. The version is pinned by `scalusVersion` in `build.sbt` — bump there when a newer snapshot is needed.

## Module layout

| Module | Platforms | Purpose |
| --- | --- | --- |
| `scalus-streaming-core` | JVM + JS | Rollback-aware streaming engine, `BlockchainStreamProvider` ADTs, query/event types, `Mailbox`, `ChainStore`/`KvChainStore`, replay sources. The hub every other module depends on. |
| `scalus-streaming-fs2` | JVM + JS | fs2 `Stream` adapter — `Fs2BlockchainStreamProvider`, `Fs2StreamingEmulator`. |
| `scalus-streaming-ox` | JVM only | softwaremill ox direct-style adapter (ox is JVM-only). |
| `scalus-cardano-network` | JVM + JS | Ouroboros Node-to-Node transport: SDU multiplexer, handshake, chain-sync, block-fetch, keep-alive. Browser JS is *not* targeted — needs raw sockets. |
| `scalus-cardano-network-it` | JVM only | Integration tests against yaci-devkit via testcontainers. `Test/fork := true`. Not published. |
| `scalus-chain-store-rocksdb` | JVM only | RocksDB-backed `ChainStore` (rocksdbjni native lib). |
| `scalus-chain-store-mithril` | JVM only | Mithril snapshot restore: `mithril-client-wasm` runs on Chicory; ImmutableDB block decoding + ledger-state ingestion. |

Source layout in cross modules: `<module>/shared/src/{main,test}/scala`, `<module>/jvm/...`, `<module>/js/...`. Single-platform modules use the flat `<module>/src/{main,test}/scala`.

## Architecture (big picture)

The streaming stack is a fan-in / fan-out pipeline:

```
Cardano node  ─►  scalus-cardano-network (N2N)        ─┐
                                                        ├─► scalus-streaming-core engine
Mithril snapshot ─► scalus-chain-store-mithril restore ─┘     │
                                                              ├─► subscriber Mailbox(es)
                                              ChainStore  ◄───┤        │
                                              (RocksDB / KV) │        ▼
                                                              └─►  flavor adapter (fs2 / ox)
                                                                         │
                                                                         ▼
                                                                  user `C[Event]` stream
```

Key separations to respect when editing:

1. **Core never depends on a stream library.** `scalus-streaming-core` exposes `MailboxSource[C]` as a typeclass over the user's stream type `C[_]`. The engine owns `Mailbox` instances and lets adapters convert them to fs2 `Stream` / ox `Source`. Don't pull `fs2`/`ox` into core.
2. **Rollback is a first-class event, not an exception.** Every event ADT has a `RolledBack(to: ChainPoint)` variant. The engine maintains a `RollbackBuffer` ~`securityParam` blocks deep; consumers either tolerate rollbacks (subscribe at tip) or wait for `confirmations` depth via `SubscriptionOptions`.
3. **Stream / one-shot duality.** `BlockchainStreamProviderTF` extends `BlockchainProviderTF`. Stream and one-shot reads share engine state — a one-shot is semantically `subscribeXxx().head`. Don't add a one-shot path that reads from a different cell than its stream counterpart; doing so reintroduces inconsistencies.
4. **Network = mini-protocol drivers behind a Multiplexer.** Each protocol (`handshake`, `chainsync`, `blockfetch`, `keepalive`) is a `*Driver` over an `AsyncByteChannel` framed by `Sdu` / `MiniProtocolBytes`. Adding a protocol = new driver + register it on the multiplexer; don't reach across into other drivers' state.
5. **ChainStore is a pluggable trait.** `KvChainStore` is the default; `RocksDbKvStore` is a `KvStore` impl that plugs in. The Mithril module restores by feeding decoded blocks through the same `ChainStore.appendBlock` pipeline an N2N stream would.

Cross-platform notes: anything in `shared/` must compile on both Scala.js and JVM — no `java.nio.channels`, no `java.util.concurrent` futures-only APIs, no rocksdb/native imports. Put JVM-only APIs (e.g. `AsyncByteChannel` socket impl) under `jvm/`.

## Environment-gated tests

Several probes/suites are gated by env vars so `sbt test` and CI don't pull large fixtures or external networks:

- `SCALUS_N2N_PREVIEW_IT=1` — `PreviewRelaySmokeSuite` (real testnet relay).
- `SCALUS_MITHRIL_FULL_PREVIEW=1` — `MithrilFullPreviewProbe` (downloads the full preview snapshot via Mithril; multi-GB).
- `SCALUS_IMMUTABLEDB_SRC=<path>` — `ImmutableDbReadProbe` / `ImmutableDbRestoreProbe` (point at an already-extracted snapshot).

Helper scripts in `scripts/` set these env vars and shell out to `sbt`:
- `download_preview.sh`     — downloads via `MithrilFullPreviewProbe`.
- `verify_and_parse_preview.sh` — SHA-256 + block-walk over an extracted snapshot.
- `restore_preview.sh`      — feeds an extracted snapshot through the full restore pipeline into a `KvChainStore` (memory-heavy; ~16 GB free recommended for preview scale).

## Style / formatting

`.scalafmt.conf`: scalafmt 3.9.4, `dialect = scala3`, `maxColumn = 100`, `indent.main = 4`, `rewrite.scala3.convertToNewSyntax = true`. The `compileAll` / `testAll` aliases run `scalafmtAll` first — keep code formatted before pushing.

Scalac flags (`commonScalacOptions`): `-deprecation`, `-feature`, `-explain`, `-Wunused:imports`, `-Xcheck-macros`. `-Wunused:imports` is on, so leaving stray imports breaks the build.

## Internal design docs

Long-form architecture and milestone notes live in `docs/local/design/`. Start with `indexer-node.md` for the public API surface and the engine's role; the milestone-specific docs (`engine-persistence-minimal.md`, `checkpoint-restart-m7.md`, `chain-store-m9.md`, `snapshot-bootstrap-m10.md`, `cardano-network-*.md`, `immutable-streaming-emulator.md`) cover individual subsystems.

## Repository rules

- Use conventional commit style: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`
- Keep messages short: 1-2 paragraphs
- Mention key changes
- Do not place attribution into commit messages and pull request.


