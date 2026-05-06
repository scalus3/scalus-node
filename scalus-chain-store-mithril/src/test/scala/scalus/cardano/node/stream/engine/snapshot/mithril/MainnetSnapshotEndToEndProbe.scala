package scalus.cardano.node.stream.engine.snapshot.mithril

import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.node.stream.engine.KvChainStore
import scalus.cardano.node.stream.engine.snapshot.{ProbeKvStore, SnapshotDirRestorer}
import scalus.cardano.node.stream.engine.snapshot.TestUtils.humanBytes
import scalus.cardano.node.stream.engine.snapshot.immutabledb.DigestsVerifier
import scalus.cardano.node.stream.engine.snapshot.ledgerstate.{LedgerStateLayout, TvarTableDecoder}

import java.io.{BufferedInputStream, FileInputStream}
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

/** End-to-end mainnet probe — the M10b capstone.
  *
  * Runs the full pipeline on a real Mithril mainnet snapshot:
  *
  *   1. Resolve aggregator + genesis-verification key (mainnet defaults; env-overridable).
  *   2. Download the latest Cardano Database V2 snapshot via [[MithrilClient]].
  *   3. Verify the certificate chain back to genesis.
  *   4. Verify file-level SHA-256 digests against the manifest ([[DigestsVerifier]]).
  *   5. Verify the Cardano-DB Merkle root against the certificate's `signed_message`
  *      ([[CardanoDatabaseVerifier]]).
  *   6. Restore the snapshot into a `RocksDbChainStore` via [[SnapshotDirRestorer]] (mandatory
  *      backend choice: mainnet's UTxO set won't fit in heap).
  *   7. Re-read the ledger-state `tables` file via [[TvarTableDecoder.streamWithTags]] to count the
  *      TxOut tag distribution and report any decode failure.
  *   8. Report per-phase wall time, peak RSS, and the final UTxO count.
  *
  * '''Manual.''' Tagged `[manual]` and gated on `SCALUS_MITHRIL_MAINNET=1`. Expected runtime is
  * many hours; expected disk footprint is hundreds of GB. Don't run casually.
  *
  * ==Required env vars==
  *   - `SCALUS_MITHRIL_MAINNET=1` — gate.
  *   - `SCALUS_RESTORE_ROCKSDB_DIR=<path>` — RocksDB backend dir for the chain store. Must be empty
  *     or absent (probe refuses to mix into an existing store; see [[ProbeKvStore]]).
  *
  * ==Optional env vars==
  *   - `SCALUS_MITHRIL_AGGREGATOR=<url>` — defaults to the official mainnet release aggregator.
  *   - `SCALUS_MITHRIL_GENESIS_KEY=<hex>` — hex-encoded JSON byte array. Defaults to mainnet's.
  *   - `SCALUS_MITHRIL_DEST=<path>` — where to land the downloaded snapshot. Default: temp dir.
  *   - `SCALUS_MITHRIL_SNAPSHOT_HASH=<h>` — pin a specific snapshot rather than `latest`.
  *
  * ==Invocation==
  * {{{
  *   SCALUS_MITHRIL_MAINNET=1 \
  *   SCALUS_MITHRIL_DEST=/data/mainnet \
  *   SCALUS_RESTORE_ROCKSDB_DIR=/data/mainnet-rocksdb \
  *     sbt 'scalusChainStoreMithril/testOnly *MainnetSnapshotEndToEndProbe'
  * }}}
  */
final class MainnetSnapshotEndToEndProbe extends AnyFunSuite {

    // Mainnet release aggregator + genesis verification key. Mirror of the ones the upstream
    // mithril-client documents on its release page.
    private val DefaultMainnetAggregator =
        "https://aggregator.release-mainnet.api.mithril.network/aggregator"
    private val DefaultMainnetGenesisKey =
        "5b3139312c36362c3134302c3138352c3133382c31312c3233372c3230372c3235302c3134342c33312c39" +
            "382c3130322c34362c3132372c3134382c3138322c35332c3138342c33382c3138362c3232382c34362c" +
            "31382c39322c3137322c3134352c3132382c39312c3134322c3132302c3133305d"

    private def resolveNetwork(): (String, String) = {
        val urlEnv = sys.env.get("SCALUS_MITHRIL_AGGREGATOR").filter(_.nonEmpty)
        val keyEnv = sys.env.get("SCALUS_MITHRIL_GENESIS_KEY").filter(_.nonEmpty)
        (urlEnv, keyEnv) match
            case (Some(u), Some(k)) => (u, k)
            case (None, None)       => (DefaultMainnetAggregator, DefaultMainnetGenesisKey)
            case (Some(_), None) =>
                fail("SCALUS_MITHRIL_AGGREGATOR set without SCALUS_MITHRIL_GENESIS_KEY")
            case (None, Some(_)) =>
                fail("SCALUS_MITHRIL_GENESIS_KEY set without SCALUS_MITHRIL_AGGREGATOR")
    }

    test(
      "[manual] download + verify + restore mainnet (requires SCALUS_MITHRIL_MAINNET=1)"
    ) {
        val enabled = sys.env.get("SCALUS_MITHRIL_MAINNET").contains("1")
        assume(enabled, "set SCALUS_MITHRIL_MAINNET=1 to run")
        assume(
          sys.env.get(ProbeKvStore.RocksDbDirEnv).isDefined,
          s"set ${ProbeKvStore.RocksDbDirEnv}=<path> — mainnet UTxO set won't fit in heap"
        )

        given ExecutionContext = ExecutionContext.global

        val (aggregatorUrl, genesisKey) = resolveNetwork()
        val destDir = sys.env
            .get("SCALUS_MITHRIL_DEST")
            .map(Path.of(_))
            .getOrElse(Files.createTempDirectory("scalus-mithril-mainnet-"))
        Files.createDirectories(destDir)

        info(s"aggregator: $aggregatorUrl")
        info(s"snapshot dir: $destDir")
        info(s"chainstore dir: ${sys.env(ProbeKvStore.RocksDbDirEnv)}")

        val rss = new RssSampler()
        rss.start()

        val client = MithrilClient.create(aggregatorUrl, genesisKey)
        try {
            // --- Phase 1: resolve metadata + download. ---
            val tDownload = System.nanoTime()
            val pinnedHash = sys.env.get("SCALUS_MITHRIL_SNAPSHOT_HASH")
            val meta = pinnedHash match
                case Some(h) =>
                    info(s"pinned snapshot hash = $h")
                    Await
                        .result(client.getCardanoDatabaseV2Snapshot(h), 60.seconds)
                        .getOrElse(fail(s"aggregator returned no snapshot for hash=$h"))
                case None =>
                    val list = Await.result(client.listCardanoDatabaseV2Snapshots(), 60.seconds)
                    val latest = list.headOption.getOrElse(fail("aggregator returned empty list"))
                    Await
                        .result(client.getCardanoDatabaseV2Snapshot(latest.hash), 60.seconds)
                        .getOrElse(fail(s"meta missing for ${latest.hash}"))

            info(
              s"tip immutable=${meta.beacon.immutableFileNumber} epoch=${meta.beacon.epoch} " +
                  s"certHash=${meta.certificateHash} totalDb=${humanBytes(meta.totalDbSizeUncompressed)}"
            )

            val lower = sys.env
                .get("SCALUS_MITHRIL_FROM")
                .map(_.toLong)
                .getOrElse(findEarliestAvailable(meta))
            val upper = sys.env
                .get("SCALUS_MITHRIL_TO")
                .map(_.toLong)
                .getOrElse(meta.beacon.immutableFileNumber)
            info(s"download range = [$lower..$upper]")

            val layout = Await.result(
              client.downloadCardanoDatabaseV2(
                meta,
                destDir,
                immutableRange = MithrilClient.ImmutableFileRange.Range(lower, upper)
              ),
              24.hours
            )
            val tDownloadMs = (System.nanoTime() - tDownload) / 1_000_000L
            info(s"download settled in ${tDownloadMs} ms")

            // --- Phase 2: cert chain. ---
            val tCert = System.nanoTime()
            val certificate =
                Await.result(client.verifyCertificateChain(meta.certificateHash), 6.hours)
            val tCertMs = (System.nanoTime() - tCert) / 1_000_000L
            info(s"cert chain verified in ${tCertMs} ms")

            // --- Phase 3: file-level digests. ---
            val tDigest = System.nanoTime()
            val manifest = DigestsVerifier.loadManifestAt(destDir)
            val fileLevel = DigestsVerifier.verifyWithCache(
              destDir.resolve("immutable"),
              manifest,
              layout.inlineDigests
            )
            val tDigestMs = (System.nanoTime() - tDigest) / 1_000_000L
            assert(
              fileLevel.presentMatchesManifest,
              s"file-level mismatches: ${fileLevel.mismatches.take(3)}"
            )
            info(
              s"digests OK in ${tDigestMs} ms (verified=${fileLevel.verified}, " +
                  s"manifest=${manifest.size})"
            )

            // --- Phase 4: Merkle root anchor. ---
            val tMerkle = System.nanoTime()
            CardanoDatabaseVerifier.verify(
              certificate,
              manifest,
              meta.beacon.immutableFileNumber
            )
            val tMerkleMs = (System.nanoTime() - tMerkle) / 1_000_000L
            info(s"Merkle root anchored in ${tMerkleMs} ms")

            // --- Phase 5: restore into RocksDB-backed chain store. ---
            val opened = ProbeKvStore.open()
            info(s"chainstore backend: ${opened.label}")
            val store = new KvChainStore(opened.store)
            val tRestore = System.nanoTime()
            val stats =
                try new SnapshotDirRestorer(store).restore(destDir)
                catch {
                    case t: Throwable =>
                        info(s"FIRST DECODE FAILURE: ${t.getClass.getSimpleName}: ${t.getMessage}")
                        throw t
                } finally store.close()
            val tRestoreMs = (System.nanoTime() - tRestore) / 1_000_000L
            info(
              s"restore in ${tRestoreMs} ms — blocks=${stats.blocks.blocksApplied}, " +
                  s"utxos=${stats.utxos.utxosRestored}, " +
                  s"bytesApplied=${humanBytes(stats.blocks.bytesApplied)}"
            )

            // --- Phase 6: TxOut tag distribution (post-restore standalone pass). ---
            val ledgerLayout = LedgerStateLayout
                .highestSlotIn(destDir.resolve("ledger"))
                .getOrElse(fail("no ledger slot dir"))
            val tablesFile = ledgerLayout match
                case LedgerStateLayout.InMemoryV2(dir, _) =>
                    dir.resolve(LedgerStateLayout.InMemoryTables)
                case other => fail(s"unexpected ledger layout: $other")

            val tTagPass = System.nanoTime()
            val (tagCounts, observedUtxos) = countTxOutTags(tablesFile)
            val tTagPassMs = (System.nanoTime() - tTagPass) / 1_000_000L
            info(s"tag-distribution pass in ${tTagPassMs} ms ($observedUtxos UTxOs walked)")

            // --- Final report. ---
            val totalMs = tDownloadMs + tCertMs + tDigestMs + tMerkleMs + tRestoreMs + tTagPassMs
            val tagReport = (0 to 7)
                .map(t => f"  tag $t : ${tagCounts.getOrElse(t, 0L)}%,d")
                .mkString("\n")
            info(
              s"\n=== MAINNET PROBE SUMMARY ===\n" +
                  s"  immutables    : [${layout.immutableRange._1}..${layout.immutableRange._2}] " +
                  s"(${layout.immutableCount})\n" +
                  s"  blocks        : ${stats.blocks.blocksApplied}\n" +
                  s"  utxos         : ${stats.utxos.utxosRestored} " +
                  s"(tag-walk observed=$observedUtxos)\n" +
                  s"  total wall    : ${totalMs / 1000} s\n" +
                  s"    download    : ${tDownloadMs / 1000} s\n" +
                  s"    cert chain  : ${tCertMs / 1000} s\n" +
                  s"    file digest : ${tDigestMs / 1000} s\n" +
                  s"    Merkle root : ${tMerkleMs / 1000} s\n" +
                  s"    restore     : ${tRestoreMs / 1000} s\n" +
                  s"    tag walk    : ${tTagPassMs / 1000} s\n" +
                  s"  peak RSS      : ${humanBytes(rss.peak)}\n" +
                  s"  TxOut tags    :\n$tagReport"
            )

            assert(stats.blocks.blocksApplied > 0, "no blocks restored")
            assert(stats.utxos.utxosRestored > 0, "no UTxOs restored")
            assert(
              observedUtxos == stats.utxos.utxosRestored,
              s"tag-walk count $observedUtxos disagrees with restore count ${stats.utxos.utxosRestored}"
            )
        } finally {
            try client.close()
            finally rss.stop()
        }
    }

    /** Walk the ledger-state `tables` file once, counting TxOut MemPack tags. */
    private def countTxOutTags(tablesFile: Path): (Map[Int, Long], Long) = {
        val counts = mutable.Map.empty[Int, Long].withDefaultValue(0L)
        var n = 0L
        val handle = TvarTableDecoder.streamWithTags(
          new BufferedInputStream(new FileInputStream(tablesFile.toFile), 1 << 17)
        )
        try {
            handle.iterator.foreach { case (_, _, tag) =>
                counts(tag) = counts(tag) + 1L
                n += 1
            }
        } finally handle.close()
        (counts.toMap, n)
    }

    /** Background sampler — pulls JVM RSS from the Hotspot OS bean every second and tracks the
      * peak. Cheap; runs as a daemon thread.
      */
    private final class RssSampler {
        private val running = new AtomicBoolean(false)
        private val peakBytes = new AtomicLong(0L)
        private var thread: Thread = scala.compiletime.uninitialized

        def start(): Unit = {
            running.set(true)
            thread = new Thread(
              () => {
                  while running.get() do {
                      val rss = currentRss()
                      val prev = peakBytes.get()
                      if rss > prev then peakBytes.compareAndSet(prev, rss)
                      try Thread.sleep(1000)
                      catch { case _: InterruptedException => () }
                  }
              },
              "scalus-mainnet-rss-sampler"
            )
            thread.setDaemon(true)
            thread.start()
        }

        def stop(): Unit = {
            running.set(false)
            if thread != null then thread.interrupt()
        }

        def peak: Long = peakBytes.get()

        /** Process RSS in bytes, or -1 if unavailable. Reads `/proc/self/status` directly so we
          * avoid the JMX OperatingSystemMXBean dance and don't depend on Hotspot internals.
          */
        private def currentRss(): Long = {
            val statusFile = Path.of("/proc/self/status")
            if !Files.exists(statusFile) then return -1L
            val src = scala.io.Source.fromFile(statusFile.toFile)
            try {
                src.getLines()
                    .find(_.startsWith("VmRSS:"))
                    .map(_.split("\\s+").apply(1).toLong * 1024L)
                    .getOrElse(-1L)
            } finally src.close()
        }
    }

    /** HEAD a candidate URL, return true on 2xx. */
    private def headOk(http: HttpClient, url: String): Boolean = {
        val req = HttpRequest
            .newBuilder(URI.create(url))
            .method("HEAD", HttpRequest.BodyPublishers.noBody())
            .build()
        val resp = http.send(req, HttpResponse.BodyHandlers.discarding())
        resp.statusCode / 100 == 2
    }

    /** Binary-search the rolling-retention window: smallest `n` in `[1..tip]` whose `immutable-N`
      * URL responds 2xx. O(log N) HEAD requests.
      */
    private def findEarliestAvailable(meta: MithrilMessages.CardanoDatabaseV2Metadata): Long = {
        val http = HttpClient.newHttpClient()
        val tip = meta.beacon.immutableFileNumber
        var lo = 1L
        var hi = tip
        while lo < hi do {
            val mid = lo + (hi - lo) / 2
            if headOk(http, MithrilClient.immutableUrl(meta, mid)) then hi = mid
            else lo = mid + 1
        }
        lo
    }

}
