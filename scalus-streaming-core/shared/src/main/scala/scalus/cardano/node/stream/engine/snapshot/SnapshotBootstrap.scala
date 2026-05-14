package scalus.cardano.node.stream.engine.snapshot

import scalus.cardano.node.stream.SnapshotSource
import scalus.cardano.node.stream.engine.ChainStore

import scala.concurrent.{ExecutionContext, Future}

/** Cold-start bootstrap: restore `source` into the [[ChainStore]] if the store is empty.
  *
  * No engine-side state is written here. The engine derives its chain-sync resume point from
  * [[scalus.cardano.node.stream.engine.Engine.resumeTip]], which falls back to `ChainStore.tip`
  * when the engine has no persisted tip of its own — so a freshly-restored store is picked up
  * without a redundant engine snapshot to keep in sync.
  *
  * Guarded on `store.tip`: a store that already has a tip is already bootstrapped (a normal restart
  * of a Heavy-mode deployment), so the restore is skipped. `ChainStoreUtxoSet.restoreUtxoSet`
  * writes the tip last, so a store left tip-less by a crashed restore is correctly re-restored.
  */
object SnapshotBootstrap {

    def run(
        source: SnapshotSource,
        store: ChainStore
    )(using ExecutionContext): Future[Unit] =
        if store.tip.isDefined then Future.unit
        else ChainStoreRestorer(store).restore(source).map(_ => ())
}
