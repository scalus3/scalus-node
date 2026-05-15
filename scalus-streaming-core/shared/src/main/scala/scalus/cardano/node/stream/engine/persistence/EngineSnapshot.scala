package scalus.cardano.node.stream.engine.persistence

import scalus.cardano.ledger.TransactionHash
import scalus.cardano.node.stream.ChainTip
import scalus.cardano.node.stream.engine.UtxoKey

/** Persisted engine state — the pair of (compacted snapshot, records appended since) that
  * [[EnginePersistenceStore.load]] returns.
  *
  * `snapshot` is `None` only on the very first run for a given `appId`. The `journal` is the
  * append-only records written after the snapshot was sealed; rebuild replays them in order against
  * the reconstructed snapshot state.
  */
final case class PersistedEngineState(
    snapshot: Option[EngineSnapshotFile],
    journal: Seq[JournalRecord]
)

/** The compacted on-disk snapshot.
  *
  * Cross-check fields (`appId`, `networkMagic`) exist to catch the "edited config without wiping
  * state" footgun early. Mismatches surface as [[EnginePersistenceError.Mismatched]] from `load`.
  *
  * `volatileTail` holds up to `securityParam` per-block summaries so the rollback buffer is
  * faithfully reconstructed — not just the tip.
  *
  * `generation` is a monotonic counter the file store bumps on every successful compaction; the
  * journal file's header carries the same generation. A mismatch ⇒ the journal is discarded on
  * load. Makes the rename-then-truncate sequence in `compact` crash-atomic. M14.C onwards. The
  * engine sets `generation = 0` when building a snapshot — the store stamps the real value before
  * writing.
  */
final case class EngineSnapshotFile(
    schemaVersion: Int,
    appId: String,
    networkMagic: Long,
    tip: Option[ChainTip],
    ownSubmissions: Set[TransactionHash],
    volatileTail: Seq[AppliedBlockSummary],
    buckets: Map[UtxoKey, BucketState],
    generation: Long = 0L
)

object EngineSnapshotFile {

    /** Bumped on incompatible on-disk format changes. Older snapshots are upgraded through the
      * [[SchemaMigration]] registry; a newer one (file written by a future library) fails to load
      * with [[EnginePersistenceError.SchemaMismatch]] — there is no forward migration.
      *
      *   - v1 (M6): the seven-field shape, no generation.
      *   - v2 (M14.C): adds [[EngineSnapshotFile.generation]].
      */
    val CurrentSchemaVersion: Int = 2
}
