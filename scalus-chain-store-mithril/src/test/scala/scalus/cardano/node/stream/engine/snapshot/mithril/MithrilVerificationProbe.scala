package scalus.cardano.node.stream.engine.snapshot.mithril

import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.node.stream.engine.snapshot.immutabledb.DigestsVerifier

import java.nio.file.{Files, Path}
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

/** End-to-end cryptographic verification probe against the real `testing-preview` aggregator.
  *
  * Exercises the full chain — `MithrilClient.verifyCertificateChain` (cert-chain walk in WASM,
  * back to the genesis verification key) → `DigestsVerifier.verify` (file-level SHA-256
  * cross-check) → `CardanoDatabaseVerifier.verify` (Merkle root anchored against the cert's
  * `signed_message`). A success here is the ground-truth confirmation that our [[MerkleMountainRange]]
  * port matches `ckb-merkle-mountain-range`'s layout (any wrong bagging direction, leaf
  * encoding, or part-key ordering would surface as `MerkleRootMismatch`).
  *
  * '''Manual.''' Tagged `[manual]` AND env-gated on `SCALUS_MITHRIL_VERIFY_PREVIEW=1` so `sbt test`
  * / CI never invoke it. Reuses an already-downloaded snapshot at `SCALUS_MITHRIL_DEST` when one
  * is present (resumable via the `.extracted` markers); otherwise downloads. Pin the snapshot
  * with `SCALUS_MITHRIL_SNAPSHOT_HASH` to verify against a previously-downloaded artifact whose
  * meta differs from the aggregator's current latest.
  *
  * Invoke with:
  * {{{
  *   SCALUS_MITHRIL_VERIFY_PREVIEW=1 \
  *   SCALUS_MITHRIL_DEST=/data/preview \
  *     sbt 'scalusChainStoreMithril/testOnly *MithrilVerificationProbe'
  * }}}
  */
final class MithrilVerificationProbe extends AnyFunSuite {

    private val aggregatorUrl =
        "https://aggregator.testing-preview.api.mithril.network/aggregator"
    private val genesisVerificationKey =
        "5b3132372c37332c3132342c3136312c362c3133372c3133312c3231332c3230372c3131372c3139382c38" +
            "352c3137362c3139392c3136322c3234312c36382c3132332c3131392c3134352c31332c3233322c3234" +
            "332c34392c3232392c322c3234392c3230352c3230352c33392c3233352c34345d"

    test(
      "[manual] verify full preview snapshot end-to-end (requires SCALUS_MITHRIL_VERIFY_PREVIEW=1)"
    ) {
        val enabled = sys.env.get("SCALUS_MITHRIL_VERIFY_PREVIEW").contains("1")
        assume(enabled, "set SCALUS_MITHRIL_VERIFY_PREVIEW=1 to run")

        given ExecutionContext = ExecutionContext.global

        val destDir = sys.env
            .get("SCALUS_MITHRIL_DEST")
            .map(Path.of(_))
            .getOrElse(Files.createTempDirectory("scalus-mithril-verify-"))
        Files.createDirectories(destDir)
        info(s"snapshot dir: $destDir")

        val client = MithrilClient.create(aggregatorUrl, genesisVerificationKey)
        try {
            val pinnedHash = sys.env.get("SCALUS_MITHRIL_SNAPSHOT_HASH")
            val meta = pinnedHash match {
                case Some(h) =>
                    info(s"pinned snapshot hash = $h")
                    Await
                        .result(client.getCardanoDatabaseV2Snapshot(h), 60.seconds)
                        .getOrElse(fail(s"aggregator returned no snapshot for hash=$h"))
                case None =>
                    val list = Await.result(client.listCardanoDatabaseV2Snapshots(), 60.seconds)
                    val latest = list.headOption.getOrElse(fail("aggregator returned empty list"))
                    info(s"latest snapshot hash = ${latest.hash}")
                    Await
                        .result(client.getCardanoDatabaseV2Snapshot(latest.hash), 60.seconds)
                        .getOrElse(fail(s"meta missing for ${latest.hash}"))
            }
            info(
              s"tip immutable=${meta.beacon.immutableFileNumber} epoch=${meta.beacon.epoch} " +
                  s"certHash=${meta.certificateHash}"
            )

            // Optional CDN-window fence — preview retains only the last ~15K chunks. Skip the
            // download step if the marker dir already holds the expected artefacts; otherwise
            // re-run download (resumable via .extracted markers).
            val lower = sys.env.get("SCALUS_MITHRIL_FROM").map(_.toLong).getOrElse(1L)
            val upper = sys.env
                .get("SCALUS_MITHRIL_TO")
                .map(_.toLong)
                .getOrElse(meta.beacon.immutableFileNumber)
            val range = MithrilClient.ImmutableFileRange.Range(lower, upper)
            info(s"download range = [$lower..$upper]")

            val tDownload = System.nanoTime()
            val layout = Await.result(
              client.downloadCardanoDatabaseV2(meta, destDir, immutableRange = range),
              12.hours
            )
            info(
              s"download settled in ${(System.nanoTime() - tDownload) / 1_000_000L}ms; " +
                  s"inline-digest cache size = ${layout.inlineDigests.size}"
            )

            val tCert = System.nanoTime()
            val certificate =
                Await.result(client.verifyCertificateChain(meta.certificateHash), 5.minutes)
            info(
              s"cert chain verified in ${(System.nanoTime() - tCert) / 1_000_000L}ms; " +
                  s"signed_message=${certificate.signedMessage}"
            )

            val manifest = DigestsVerifier.loadManifestAt(destDir)
            info(s"digests manifest entries: ${manifest.size}")

            val tFile = System.nanoTime()
            val fileLevel = DigestsVerifier.verifyWithCache(
              destDir.resolve("immutable"),
              manifest,
              layout.inlineDigests
            )
            info(
              s"file-level verify in ${(System.nanoTime() - tFile) / 1_000_000L}ms: " +
                  s"verified=${fileLevel.verified} mismatches=${fileLevel.mismatches.size} " +
                  s"missingOnDisk=${fileLevel.missingOnDisk.size} " +
                  s"unexpectedOnDisk=${fileLevel.unexpectedOnDisk.size}"
            )
            assert(fileLevel.presentMatchesManifest, s"file-level mismatches: ${fileLevel.mismatches.take(3)}")

            val tMerkle = System.nanoTime()
            CardanoDatabaseVerifier.verify(
              certificate,
              manifest,
              meta.beacon.immutableFileNumber
            )
            info(
              s"✓ Merkle root anchored — recomputed signed_message matches certificate " +
                  s"(${(System.nanoTime() - tMerkle) / 1_000_000L}ms)"
            )
        } finally client.close()
    }
}
