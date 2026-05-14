package scalus.cardano.node.stream

import scalus.cardano.ledger.CardanoInfo
import scalus.cardano.node.BlockchainProvider
import scalus.cardano.node.stream.engine.{ChainStore, ChainStoreOwnSubmissions}
import scalus.cardano.node.stream.engine.persistence.EnginePersistenceStore
import scalus.cardano.node.stream.engine.replay.{ChainStoreReplaySource, ReplaySource}

/** Configuration for a streaming provider.
  *
  * `appId` is a reverse-DNS identifier for this application instance (e.g. `com.mycompany.batcher`)
  * and names the engine's persistent state location. Must be non-empty so two Scalus apps on the
  * same machine don't collide on state files. See *App identity and persistence location* in
  * `docs/local/claude/indexer/indexer-node.md`.
  *
  * `cardanoInfo` names the network and seeds the protocol-params cell. `chainSync` picks the
  * live-event source; `backup` answers historical and out-of-window queries that the engine cannot
  * serve from local state; `storage` selects the engine's memory/durability profile *and* its
  * persistence backend — see [[StorageProfile]].
  *
  * See the indexer design doc at `docs/local/claude/indexer/indexer-node.md` for milestone-level
  * context.
  */
case class StreamProviderConfig(
    appId: String,
    cardanoInfo: CardanoInfo,
    chainSync: ChainSyncSource,
    backup: BackupSource,
    storage: StorageProfile = StorageProfile.Light()
) {
    require(appId.nonEmpty, "StreamProviderConfig.appId must be non-empty")

    /** The durable block-history store, if this is a [[StorageProfile.Heavy]] deployment. */
    def chainStore: Option[ChainStore] = storage match {
        case StorageProfile.Heavy(cs, _) => Some(cs)
        case _: StorageProfile.Light     => None
    }

    /** The cold-start snapshot source, if configured. Structurally Heavy-only — `bootstrap` is a
      * field of [[StorageProfile.Heavy]], so it cannot exist without a `chainStore` to restore
      * into.
      */
    def bootstrap: Option[SnapshotSource] = storage match {
        case StorageProfile.Heavy(_, b) => b
        case _: StorageProfile.Light    => None
    }

    /** Replay sources derived from this config, in the order the engine should try them after the
      * rollback buffer. Today that's just the optional `chainStore`; Phase 2b will add a peer
      * source when `chainSync` is an N2N configuration.
      */
    def fallbackReplaySources: List[ReplaySource] =
        chainStore.toList.map(new ChainStoreReplaySource(_))
}

/** Where live chain events come from. */
sealed trait ChainSyncSource
object ChainSyncSource {

    /** No protocol — events are pushed in by tests or the emulator. */
    case object Synthetic extends ChainSyncSource

    /** Connect to a public relay over Ouroboros Node-to-Node (TCP).
      *
      * @param networkMagic
      *   32-bit Cardano network identifier, sent in the handshake. Mainnet = 764824073, Preview
      *   = 2, Preprod = 1, yaci-devkit default = 42. A plain `Long` (not the opaque
      *   `scalus.cardano.network.NetworkMagic`) to keep this ADT free of a dependency on the
      *   cardano-network module. The JVM provider wraps it into `NetworkMagic` at the boundary.
      *
      * Wiring for this variant is JVM-only — the JS build of `Fs2BlockchainStreamProvider`
      * still raises `UnsupportedSourceException` because `NodeToNodeClient` requires raw TCP
      * which Scala.js doesn't provide out of the box.
      */
    case class N2N(host: String, port: Int, networkMagic: Long) extends ChainSyncSource

    /** Connect to a local cardano-node over Node-to-Client (Unix-domain socket). JVM-only.
      *
      * @param socketPath
      *   absolute filesystem path to the cardano-node `.socket`. String (not `java.nio.file.Path`)
      *   so the case class stays JS-compatible — the JVM provider wraps it via `Path.of(...)` at
      *   the boundary.
      * @param networkMagic
      *   32-bit Cardano network identifier sent in the N2C handshake. Mainnet = 764824073, Preview =
      *   2, Preprod = 1, yaci-devkit default = 42. Same `Long` shape as [[N2N.networkMagic]] for
      *   symmetry.
      */
    case class N2C(socketPath: String, networkMagic: Long) extends ChainSyncSource
}

/** Where the engine looks when it cannot answer a snapshot query from its own state (historical
  * UTxOs, tx status outside the rollback buffer, submit before N2N `TxSubmission2` lands, …).
  */
sealed trait BackupSource
object BackupSource {

    /** Bloxbean's `BlockfrostProvider` — answers both historical UTxO seeding and `submit` in the
      * M2 stack.
      */
    case class Blockfrost(
        apiKey: String,
        network: BlockfrostNetwork,
        maxConcurrentRequests: Int = 5
    ) extends BackupSource

    /** Full-feature backup over Node-to-Client: `LocalTxSubmission` for `submit` (since M11.P3),
      * `LocalStateQuery` for `currentSlot` / `fetchLatestParams` / `findUtxos` (since M12.P1–P3),
      * `LocalTxMonitor` for `checkTransaction` (since M12.P4). JVM-only.
      *
      * `fetchLatestParams` is Conway-only; `findUtxos` supports the single-source shapes
      * `Simple(FromAddress(_))` and `Simple(FromInputs(_))` and returns
      * [[UtxoQueryError.NotSupported]] for richer shapes — pair with `BackupSource.Blockfrost` when
      * broader UtxoQuery support is needed. `checkTransaction` returns `Pending` when the tx is in
      * the local mempool snapshot and `NotFound` otherwise; the `Confirmed` answer for on-chain
      * transactions comes from the engine's own `TxHashIndex` ahead of falling through to this
      * backup (see `BaseStreamProvider.checkTransaction`).
      *
      * @param socketPath
      *   absolute filesystem path to the cardano-node `.socket`. Same shape as
      *   [[ChainSyncSource.N2C.socketPath]]; configuring the same path on both shares the socket
      *   intent (single connection sharing is a planned optimisation; today each component opens
      *   its own).
      * @param networkMagic
      *   32-bit Cardano network identifier sent in the N2C handshake.
      */
    case class LocalNode(socketPath: String, networkMagic: Long) extends BackupSource

    /** Escape hatch — pass an existing `BlockchainProvider`. Useful for tests, chained providers,
      * or custom backends.
      */
    case class Custom(provider: BlockchainProvider) extends BackupSource

    /** Explicit no-backup. Snapshot methods that would need the backup return a typed "no source
      * configured" error. Suitable for write-only chain followers and fresh-script use cases where
      * there are genuinely no historical UTxOs.
      */
    case object NoBackup extends BackupSource
}

/** Blockfrost has three separate API hosts. `Network` from `scalus.cardano.address` only
  * distinguishes `Testnet` from `Mainnet` so we need our own tag to pick between the two testnets.
  */
enum BlockfrostNetwork {
    case Mainnet, Preview, Preprod
}

/** How a deployment persists engine state — and, for Heavy mode, where its durable block/UTxO
  * history lives. Exactly one of the two shapes: configuring "both backends" is unrepresentable.
  */
sealed trait StorageProfile
object StorageProfile {

    /** Light: file-backed engine persistence, no durable chain store. Per-active-subscription UTxO
      * indexes only; the rollback buffer is always present. Memory bounded by `securityParam` ×
      * subscription footprint. The default.
      *
      * `enginePersistence` controls warm-restart durability. `null` (the default) asks the factory
      * to wire the platform-appropriate default — a file-backed store keyed by `appId` on the JVM;
      * JS adapters may fall back to `.noop`. Explicitly passing `EnginePersistenceStore.noop` opts
      * into Cold-restart semantics (tests, demos, one-shots).
      */
    case class Light(enginePersistence: EnginePersistenceStore | Null = null) extends StorageProfile

    /** Heavy: a [[ChainStore]] is the *single* persistence backend and the durable block/UTxO
      * source. `findUtxos` becomes a local lookup for any query; checkpoint replay past the
      * rollback-buffer horizon is served from the store. Multi-GB on mainnet; practically requires
      * a `bootstrap` snapshot source.
      *
      * The store must implement [[scalus.cardano.node.stream.engine.ChainStoreOwnSubmissions]]: in
      * Heavy mode the ChainStore is the sole persistence layer (there is no file store alongside),
      * so it has to hold the engine's `ownSubmissions` set. `KvChainStore` — including the
      * RocksDB-backed flavour — qualifies.
      *
      * @param chainStore
      *   the durable store.
      * @param bootstrap
      *   optional cold-start snapshot source, restored into `chainStore` when it is empty.
      *   Structurally cannot exist without a `chainStore`.
      */
    case class Heavy(
        chainStore: ChainStore & ChainStoreOwnSubmissions,
        bootstrap: Option[SnapshotSource] = None
    ) extends StorageProfile
}

/** Signalled by the factory when a [[ChainSyncSource]] or [[BackupSource]] is named in
  * configuration but the milestone that implements it has not landed yet.
  */
final case class UnsupportedSourceException(message: String) extends RuntimeException(message)
