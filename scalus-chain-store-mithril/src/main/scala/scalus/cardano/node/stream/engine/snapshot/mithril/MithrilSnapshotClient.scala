package scalus.cardano.node.stream.engine.snapshot.mithril

import scalus.cardano.node.stream.engine.snapshot.MithrilSnapshotResolver

/** Compatibility shim. Resolution of [[scalus.cardano.node.stream.SnapshotSource.Mithril]] is now
  * driven by [[MithrilSnapshotResolver]], discovered from the classpath via `ServiceLoader` and
  * implemented in [[MithrilSnapshotResolverImpl]]. Kept as a tiny direct entry point for callers
  * that already depend on `scalus-chain-store-mithril` and want to skip the SPI lookup.
  */
object MithrilSnapshotClient {

    /** Direct handle on the resolver. Equivalent to `MithrilSnapshotResolver.find().get` when
      * `scalus-chain-store-mithril` is on the classpath, but skips ServiceLoader.
      */
    def resolver: MithrilSnapshotResolver = new MithrilSnapshotResolverImpl
}
