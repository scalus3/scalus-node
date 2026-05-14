package scalus.cardano.node.stream.engine.persistence

import scalus.cardano.node.stream.engine.{ChainStore, ChainStoreOwnSubmissions}

import scala.concurrent.Future

/** [[EnginePersistenceStore]] backed by a [[ChainStore]] — the persistence story for Heavy-mode
  * deployments ([[scalus.cardano.node.stream.StorageProfile.Heavy]]).
  *
  * In Heavy mode the ChainStore is *already* the durable source of truth: it persists the block
  * history, the full UTxO set, and the chain tip, kept current by the engine's `appendBlock` /
  * `rollbackTo` calls on every roll-forward / roll-backward. The only engine-local state it does
  * not already hold is `ownSubmissions` — txs this app submitted, tracked for `Pending` — so this
  * adapter routes just that to a dedicated ChainStore keyspace ([[ChainStoreOwnSubmissions]]) and
  * derives everything else from the store. There is no second on-disk format, and therefore no
  * engine-snapshot / ChainStore tip to reconcile.
  *
  *   - `load` — `None` when the store is empty (cold start). Otherwise an [[EngineSnapshotFile]]
  *     synthesised from `ChainStore.tip` + `ownSubmissions`. `volatileTail` and `buckets` are
  *     empty: the rollback buffer refills from live chain-sync and buckets re-seed lazily per
  *     subscription (see the M14 design doc for the empty-`volatileTail` trade-off and its planned
  *     follow-up).
  *   - `appendSync` — `OwnSubmitted` / `OwnForgotten` hit the `ownSubmissions` keyspace; `Forward`
  *     / `Backward` are no-ops because the engine already drives the ChainStore directly via
  *     `appendBlock` / `rollbackTo`.
  *   - `flush` / `compact` — no-ops: the ChainStore is a live KV store, always durable.
  *   - `close` — no-op: the provider owns the ChainStore lifecycle and closes it separately.
  */
final class ChainStorePersistenceStore(
    store: ChainStore & ChainStoreOwnSubmissions,
    appId: String,
    networkMagic: Long
) extends EnginePersistenceStore {

    def load(): Future[Option[PersistedEngineState]] = Future.successful {
        store.tip.map { t =>
            PersistedEngineState(
              snapshot = Some(
                EngineSnapshotFile(
                  schemaVersion = EngineSnapshotFile.CurrentSchemaVersion,
                  appId = appId,
                  networkMagic = networkMagic,
                  tip = Some(t),
                  ownSubmissions = store.ownSubmissions,
                  volatileTail = Seq.empty,
                  buckets = Map.empty
                )
              ),
              journal = Seq.empty
            )
        }
    }

    def appendSync(record: JournalRecord): Unit = record match {
        case JournalRecord.OwnSubmitted(hash) => store.putOwnSubmission(hash)
        case JournalRecord.OwnForgotten(hash) => store.deleteOwnSubmission(hash)
        case _: JournalRecord.Forward         => ()
        case _: JournalRecord.Backward        => ()
    }

    def flush(): Future[Unit] = Future.unit

    def compact(snap: EngineSnapshotFile): Future[Unit] = Future.unit

    def close(): Future[Unit] = Future.unit
}
