# scalus-node

Embedded Cardano node modules for [Scalus](https://github.com/scalus3/scalus): a
rollback-aware streaming engine, Cardano network protocols (Node-to-Node),
ChainStore back-ends (RocksDB), and Mithril-verified snapshot restore.

This repo was extracted from `scalus3/scalus` (subdirectory `scalus-embedded-node/`)
with full per-file commit history preserved. It depends on the published
`org.scalus` artifacts as binary dependencies.

## Modules

- `scalus-streaming-core` — rollback-aware `BlockchainStreamProvider` engine, ADTs, chain-sync adapters (JVM + JS).
- `scalus-streaming-fs2` — fs2 flavor (JVM + JS).
- `scalus-streaming-ox` — ox flavor (JVM only).
- `scalus-cardano-network` — Ouroboros N2N transport: mini-protocol mux, handshake, keep-alive (JVM + JS).
- `scalus-cardano-network-it` — yaci-devkit testcontainers integration tests (JVM only).
- `scalus-chain-store-rocksdb` — RocksDB-backed `ChainStore` (JVM only).
- `scalus-chain-store-mithril` — Mithril-verified snapshot restore via embedded `mithril-client-wasm` on Chicory.

## Building

Snapshots of `org.scalus` artifacts are pulled from
[Sonatype Central Snapshots](https://central.sonatype.com/repository/maven-snapshots/).
The current pinned scalus version is set by `scalusVersion` in `build.sbt`.

```
sbt jvm/compile
sbt jvm/test
```
