# scalus-node

Embedded Cardano node modules for [Scalus](https://github.com/scalus3/scalus): a
rollback-aware streaming engine, Cardano network protocols (Node-to-Node and
Node-to-Client), ChainStore back-ends (RocksDB), and Mithril-verified snapshot
restore.

## Modules

- `scalus-streaming-core` — rollback-aware `BlockchainStreamProvider` engine, ADTs, chain-sync adapters (JVM + JS).
- `scalus-streaming-fs2` — fs2 flavor (JVM + JS).
- `scalus-streaming-ox` — ox flavor (JVM only).
- `scalus-cardano-network` — Ouroboros N2N (TCP) + N2C (Unix-domain socket) transports: mini-protocol mux, handshakes, keep-alive, chain-sync, local-tx-submission (JVM + JS, JS stubs raise on connect).
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

## Deployment topologies

A streaming provider is configured by two sources: `ChainSyncSource` (where live
chain events come from) and `BackupSource` (where snapshot reads and `submit`
fall through when the engine can't answer locally).

### N2N + Blockfrost (the historic shape)

Most public-relay deployments. Live chain events come from a public N2N relay over
TCP; historical reads and `submit` go through Blockfrost.

```scala
StreamProviderConfig(
  appId       = "com.example.app",
  cardanoInfo = CardanoInfo.preview,
  chainSync   = ChainSyncSource.N2N(host, port, networkMagic),
  backup      = BackupSource.Blockfrost(apiKey, BlockfrostNetwork.Preview)
)
```

### All-N2C (M11 and later — preferred when a local cardano-node is available)

Deployments that co-locate with a `cardano-node` over its Unix-domain socket.
Live chain events come via N2C ChainSync; `submit` goes via LocalTxSubmission.
JVM-only (UDS).

```scala
val socket = "/var/run/cardano-node/preview.socket"
StreamProviderConfig(
  appId       = "com.example.app",
  cardanoInfo = CardanoInfo.preview,
  chainSync   = ChainSyncSource.N2C(socket, networkMagic),
  backup      = BackupSource.LocalNode(socket, networkMagic)
)
```

`BackupSource.LocalNode` is **submit-only** until M12 (LocalStateQuery): its read
methods (`findUtxos`, `fetchLatestParams`, `currentSlot`, `getDatum`,
`checkTransaction`) raise `UnsupportedOperationException`. Use the engine's own
state for anything answerable from the rollback buffer + `ChainStore`, or pair
with a `BackupSource.Blockfrost` for read coverage during the M11 window.

When `ChainSyncSource.N2C` and `BackupSource.LocalNode` reference the same
socket path, sharing the connection is a planned optimisation; today each
component opens its own.

### Migrating Blockfrost → all-N2C

For deployments that already have a local `cardano-node`:

1. Keep `ChainSyncSource.N2N(...)` running side-by-side with the new N2C
   provider during the migration window. The engine state is identical between
   the two sync sources; you can flip-test without warm-restart loss.
2. Once `ChainSyncSource.N2C(socketPath, networkMagic)` is wired and you've
   verified subscribers see the expected blocks/rollbacks (e.g. via
   `subscribeTip()`), retire the N2N config.
3. Replace `BackupSource.Blockfrost(...)` with `BackupSource.LocalNode(socketPath, networkMagic)`
   only after auditing every snapshot-method call site in your application —
   pre-M12 reads will fail. If you depend on `findUtxos` or `fetchLatestParams`,
   either keep Blockfrost as the backup or layer an HTTPS read shim until M12
   ships LSQ.
4. Read deployment health via `provider.backupDiagnostics`:
   `connectedSinceMillis`, `lastSubmittedHash`, `submitCount`, `rejectCount`.
   `LocalNodeProvider` implements the trait; Blockfrost returns `None` today.
