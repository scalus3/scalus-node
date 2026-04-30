package scalus.cardano.node.stream.engine.snapshot

import scalus.cardano.node.stream.{ChainTip, SnapshotSource}
import scalus.cardano.node.stream.engine.{ChainStore, ChainStoreUtxoSet}

import scala.concurrent.{ExecutionContext, Future}

/** SPI for the Mithril-shaped snapshot acquisition path. The streaming-core module owns the
  * ChainStoreRestorer dispatch but does not depend on the (JVM-only) `scalus-chain-store-mithril`
  * module that knows how to download cardano-node V2 artefacts and unpack ImmutableDB +
  * ledger-state. Implementations are discovered via `java.util.ServiceLoader` at restore time, so
  * applications that put `scalus-chain-store-mithril` on the classpath get end-to-end Mithril
  * support transparently.
  *
  * On Scala.js — and on JVM where the mithril module is not on the classpath —
  * [[MithrilSnapshotResolver.find]] returns `None`, and the
  * [[ChainStoreRestorer]] dispatch fails the restore Future with
  * [[scalus.cardano.node.stream.UnsupportedSourceException]] pointing at M10b.
  */
trait MithrilSnapshotResolver {

    /** Restore from a [[SnapshotSource.Mithril]] (download + verify + unpack + populate the store).
      * Implementations own the work directory, HTTP client, and WASM lifetime. The returned tip is
      * the snapshot's anchor — same value the synchronous CBOR path returns from
      * [[ChainStoreRestorer.restore]].
      */
    def restore(
        source: SnapshotSource.Mithril,
        store: ChainStore & ChainStoreUtxoSet
    )(using ExecutionContext): Future[ChainTip]

    /** Restore from a [[SnapshotSource.MithrilDir]] (already-extracted cardano-node snapshot
      * directory). Skips download + cryptographic verification — this is the trusted-fixture /
      * external-pipeline path.
      */
    def restoreDir(
        source: SnapshotSource.MithrilDir,
        store: ChainStore & ChainStoreUtxoSet
    )(using ExecutionContext): Future[ChainTip]
}

object MithrilSnapshotResolver {

    /** ServiceLoader-based discovery. Returns the first registered resolver (there's only one in
      * practice — `scalus-chain-store-mithril`'s `MithrilSnapshotResolverImpl`). Wrapped in a
      * defensive try/catch so platforms without ServiceLoader (Scala.js partial impl) silently
      * return `None` instead of failing class load.
      */
    def find(): Option[MithrilSnapshotResolver] = {
        try {
            val it = java.util.ServiceLoader
                .load(classOf[MithrilSnapshotResolver])
                .iterator()
            if it.hasNext then Some(it.next()) else None
        } catch {
            case _: Throwable => None
        }
    }
}
