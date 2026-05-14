package scalus.cardano.node.stream.engine

import scalus.cardano.ledger.{DataHash, TransactionHash, TransactionInput, TransactionOutput, Utxos}
import scalus.cardano.node.UtxoQuery
import scalus.cardano.node.stream.{ChainPoint, ChainTip}
import scalus.cardano.node.stream.engine.replay.ReplayError
import scalus.uplc.builtin.Data

/** Pluggable durable block-history store.
  *
  * An application that wants checkpoint replay from arbitrarily old points — or
  * [[scalus.cardano.node.stream.StorageProfile.Heavy]] mode (M9.P3) — provides a `ChainStore`
  * implementation via [[scalus.cardano.node.stream.StreamProviderConfig.chainStore]].
  *
  * The trait covers both the read path (historical blocks for [[replay.ChainStoreReplaySource]],
  * used by M7 checkpoint replay) and the write path needed to keep the store populated from the
  * live chain-sync loop. When a `ChainStore` is configured, the engine calls [[appendBlock]] on
  * every `onRollForward` and [[rollbackTo]] on every `onRollBackward`; the engine worker is the
  * only writer.
  *
  * Implementations are not required to be thread-safe — the engine serialises every access through
  * its single worker thread.
  *
  * See `docs/local/claude/indexer/chain-store-m9.md` for the full design.
  */
trait ChainStore {

    /** Blocks strictly after `from` and up to and including `to`. Returns
      * `Left(ReplaySourceExhausted(point))` if the store does not cover `from` (e.g. `from`
      * pre-dates the store's horizon, or the chain forked off and `from` is on a dead branch).
      */
    def blocksBetween(
        from: ChainPoint,
        to: ChainPoint
    ): Either[ReplayError.ReplaySourceExhausted, Iterator[AppliedBlock]]

    /** Persist `block` at its point. Idempotent on duplicate `(slot, hash)` — a restart that
      * re-applies already-seen blocks must not double-count. The engine calls this after updating
      * its own state in `onRollForward`, so errors here do not affect live fan-out.
      */
    def appendBlock(block: AppliedBlock): Unit

    /** Forget every stored block whose point is strictly after `to`. No-op if `to` equals (or is
      * ahead of) the current tip. Called from `onRollBackward`.
      */
    def rollbackTo(to: ChainPoint): Unit

    /** Current tip of the store, or `None` if empty. Consulted at engine start for warm-resume
      * diagnostics; the engine does NOT feed it back into its own tip cell (the engine's tip comes
      * from `EnginePersistenceStore` in M6 and live chain-sync).
      */
    def tip: Option[ChainTip]

    /** Drop blocks older than `horizon`. Default is a no-op — stores retain everything unless the
      * caller explicitly bounds disk. Apps that want a sliding-window store override this and wire
      * it into their own housekeeping loop.
      */
    def pruneBefore(horizon: ChainPoint): Unit = ()

    /** Release any resources (file handles, native allocations). Idempotent. The provider calls
      * `close()` from its `preClose` teardown hook, after the chain-sync loop has stopped.
      */
    def close(): Unit
}

/** Optional mixin for [[ChainStore]] backends that also maintain a queryable UTxO set — the thing
  * [[scalus.cardano.node.stream.StorageProfile.Heavy]] depends on.
  *
  * Not every ChainStore needs this. A block-history-only store (M7 replay via a file archive, a
  * minimal SQL dump) legitimately doesn't, and the engine falls through to the configured backup as
  * before. A store that does implement this trait must keep the UTxO set consistent with the block
  * history across `appendBlock` / `rollbackTo` — the engine treats the two as co-authoritative for
  * Heavy mode.
  */
trait ChainStoreUtxoSet { self: ChainStore =>

    /** Query UTxOs matching `q` from the local store. `None` means "not answerable locally" — the
      * caller (usually [[Engine.findUtxosLocal]]) falls through to the configured backup.
      * `Some(utxos)` is authoritative, even if empty.
      */
    def findUtxosFromStore(q: UtxoQuery): Option[Utxos]

    /** Bulk-replace the UTxO set with `utxos`, anchoring it at `tip`. Consumed by M10's
      * `ChainStoreRestorer` when the store bootstraps from a snapshot; also available to apps that
      * manage their own restore pipeline.
      *
      * **Not required to be atomic.** A mainnet-sized UTxO set can't fit in a single write batch on
      * every backend, so implementations stream the restore in chunks. A crash mid-restore leaves
      * the store in a partial state; callers must treat it as discardable-and-re-runnable (wipe and
      * re-invoke `restoreUtxoSet`). Implementations MUST write `tip` last, so a partially-restored
      * store has no tip and is observably invalid — the provider's cold-start guard (no persisted
      * tip) triggers a fresh bootstrap on the next run.
      */
    def restoreUtxoSet(
        tip: ChainTip,
        utxos: Iterator[(TransactionInput, TransactionOutput)]
    ): Unit
}

/** Optional mixin for [[ChainStore]] backends that also retain a `DataHash -> Data` dictionary of
  * every datum observed in the block history — inline output datums plus witness-set `plutusData`
  * entries.
  *
  * Backs `BlockchainReader.getDatum` for hashes that aged out of the engine's volatile
  * [[DatumIndex]] (or were witnessed before the subscriber joined) on deployments without a
  * historical-query backup. Datums are content-addressed, so the dictionary is monotonic — a
  * rolled-back block does not invalidate the data it contributed; if some other branch reintroduces
  * the same hash, the value is identical. Implementations therefore do not need to remove entries
  * on `rollbackTo`, and may share the same dictionary across forks.
  */
trait ChainStoreDatumDict { self: ChainStore =>

    /** Look up a datum by hash. Returns `None` if no block previously committed via
      * [[ChainStore.appendBlock]] contributed this hash, in which case the caller falls through to
      * the configured backup.
      */
    def getDatumFromStore(hash: DataHash): Option[Data]
}

/** Optional mixin for [[ChainStore]] backends that can also persist the engine's `ownSubmissions`
  * set — the hashes of transactions *this app* submitted and is still tracking for `Pending`.
  *
  * This is the one piece of warm-restart state that is not chain data, so it has no natural home in
  * the block / UTxO keyspaces. A `ChainStore` that implements this trait can serve as the engine's
  * *sole* persistence backend in Heavy-mode deployments (via
  * [[scalus.cardano.node.stream.engine.persistence.ChainStorePersistenceStore]]) — everything else
  * the engine needs to warm-restart (tip, block history) the store already holds. A `ChainStore`
  * that does not implement it still works for chain queries; the deployment then pairs it with a
  * file-backed `EnginePersistenceStore` as before.
  *
  * All three methods are called only from the engine worker thread — no thread-safety burden.
  */
trait ChainStoreOwnSubmissions { self: ChainStore =>

    /** Every currently-persisted own-submission hash. */
    def ownSubmissions: Set[TransactionHash]

    /** Record `hash` as an own submission. Idempotent. */
    def putOwnSubmission(hash: TransactionHash): Unit

    /** Forget `hash`. No-op if it was never recorded. */
    def deleteOwnSubmission(hash: TransactionHash): Unit
}

object ChainStore {

    /** No-op read-only store — the M7 placeholder. Every `blocksBetween` returns `Left(exhausted)`;
      * writes are discarded. Useful when the config asks for a ChainStore wire but the backend
      * isn't ready yet.
      */
    val noop: ChainStore = new ChainStore {
        def blocksBetween(
            from: ChainPoint,
            to: ChainPoint
        ): Either[ReplayError.ReplaySourceExhausted, Iterator[AppliedBlock]] =
            Left(ReplayError.ReplaySourceExhausted(from))

        def appendBlock(block: AppliedBlock): Unit = ()
        def rollbackTo(to: ChainPoint): Unit = ()
        def tip: Option[ChainTip] = None
        def close(): Unit = ()
    }
}
