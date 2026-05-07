package scalus.cardano.node.stream.engine.snapshot.immutabledb

import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.node.stream.engine.KvChainStore
import scalus.cardano.node.stream.engine.snapshot.ProbeKvStore
import scalus.cardano.node.stream.engine.snapshot.TestUtils.humanBytes

import java.nio.file.{Files, Path}

/** Manual probe: restore the full extracted preview snapshot into a freshly-allocated
  * `KvChainStore` and print timing + final tip. Intended to confirm the decode-and-apply pipeline
  * works at preview scale (~2.37 M blocks / ~5.35 GB of CBOR).
  *
  * ==Backend selection==
  *
  * By default the probe uses a heap-resident `InMemoryKvStore` — expect ~15+ GB RSS peak on
  * preview, and impossible at mainnet scale. Set
  * [[ProbeKvStore.RocksDbDirEnv `SCALUS_RESTORE_ROCKSDB_DIR=<path>`]] to back the store with
  * RocksDB at `<path>` instead; that directory must be empty or absent (we refuse to overwrite
  * existing data).
  *
  * Invoke:
  * {{{
  *   SCALUS_IMMUTABLEDB_SRC=/tmp/mithrill-preview \
  *     sbt 'scalusChainStoreMithril/testOnly *ImmutableDbRestoreProbe'
  *
  *   # Or, RocksDB-backed (required for mainnet-scale runs):
  *   SCALUS_IMMUTABLEDB_SRC=/tmp/mithrill-preview \
  *   SCALUS_RESTORE_ROCKSDB_DIR=/tmp/scalus-restore-rocksdb \
  *     sbt 'scalusChainStoreMithril/testOnly *ImmutableDbRestoreProbe'
  * }}}
  */
final class ImmutableDbRestoreProbe extends AnyFunSuite {

    test("[manual] restore preview into KvChainStore (in-memory or RocksDB), print tip") {
        val src = sys.env.get("SCALUS_IMMUTABLEDB_SRC").map(Path.of(_))
        assume(src.isDefined, "set SCALUS_IMMUTABLEDB_SRC=<extracted-snapshot-dir>")
        val immutable = src.get.resolve("immutable")
        assume(Files.isDirectory(immutable), s"need $immutable")

        val opened = ProbeKvStore.open()
        info(s"backend: ${opened.label}")
        val store = new KvChainStore(opened.store)
        try {
            val stats = new ImmutableDbRestorer(store).restore(
              immutable,
              onProgress = p =>
                  if p.chunkIndex % 500 == 0 || p.chunkIndex == p.totalChunks - 1 then
                      info(
                        s"  chunk ${p.chunkIndex + 1}/${p.totalChunks} (n=${p.chunkNo}): " +
                            s"${p.cumulativeBlocks} blocks, ${humanBytes(p.cumulativeBytes)}"
                      )
            )

            info(
              s"=== SUMMARY ===\n" +
                  s"  chunks        = ${stats.chunksProcessed}\n" +
                  s"  blocks        = ${stats.blocksApplied}\n" +
                  s"  bytes applied = ${humanBytes(stats.bytesApplied)}\n" +
                  s"  skippedByron  = ${stats.skippedByron}\n" +
                  s"  elapsed       = ${stats.elapsedMillis} ms\n" +
                  s"  throughput    = ${humanBytes(stats.bytesApplied * 1000L / math.max(stats.elapsedMillis, 1L))}/s\n" +
                  s"  tip           = ${stats.tip}\n" +
                  s"  store.tip     = ${store.tip}"
            )
            assert(stats.tip.isDefined)
            assert(store.tip.contains(stats.tip.get))
        } finally store.close()
    }
}
