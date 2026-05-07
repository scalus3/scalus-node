package scalus.cardano.node.stream.engine.snapshot.mithril

import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.node.stream.engine.snapshot.immutabledb.DigestsVerifier
import scalus.utils.Hex.toHex

import java.nio.file.{Files, Path}
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

/** Fast cross-validation of the [[MerkleMountainRange]] port against the real testing-preview
  * aggregator: fetch snapshot metadata, recompute the Cardano-Database Merkle root locally over the
  * same digests manifest the snapshot ships, and compare against the aggregator's published
  * `merkleRoot` field. No certificate chain walk, no WASM cryptography — runs in seconds.
  *
  * Why this works as ground-truth: `CardanoDatabaseV2Metadata.merkleRoot` is the exact value
  * `mithril-stm` produces by running its MKTree (the `ckb-merkle-mountain-range` MMR with
  * Blake2s256 internal merge) over the digests in the manifest. If our port matches upstream's leaf
  * encoding, ordering, internal merge, and right-to-left bagging, the recomputed root must equal
  * the published one byte-for-byte. A mismatch dumps both hex strings so the diagnostic is
  * explicit.
  *
  * '''Manual.''' Tagged `[manual]` AND env-gated on `SCALUS_MITHRIL_VERIFY_PREVIEW=1`. Reuses an
  * already-downloaded snapshot at `SCALUS_MITHRIL_DEST` when one is present (only the digests
  * manifest is required — a few MB, downloaded if missing).
  */
final class MithrilMerkleRootProbe extends AnyFunSuite {

    private val aggregatorUrl =
        "https://aggregator.testing-preview.api.mithril.network/aggregator"
    private val genesisVerificationKey =
        "5b3132372c37332c3132342c3136312c362c3133372c3133312c3231332c3230372c3131372c3139382c38" +
            "352c3137362c3139392c3136322c3234312c36382c3132332c3131392c3134352c31332c3233322c3234" +
            "332c34392c3232392c322c3234392c3230352c3230352c33392c3233352c34345d"

    test(
      "[manual] preview snapshot's merkleRoot matches our local MMR computation"
    ) {
        val enabled = sys.env.get("SCALUS_MITHRIL_VERIFY_PREVIEW").contains("1")
        assume(enabled, "set SCALUS_MITHRIL_VERIFY_PREVIEW=1 to run")

        given ExecutionContext = ExecutionContext.global

        val destDir = sys.env
            .get("SCALUS_MITHRIL_DEST")
            .map(Path.of(_))
            .getOrElse(Files.createTempDirectory("scalus-mithril-merkle-root-"))
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
                  s"published merkleRoot=${meta.merkleRoot}"
            )

            // Make sure the digests manifest is available locally — download just that archive
            // if the dir doesn't already have one. The full immutable set isn't needed for this
            // probe; only the manifest's hex digest values feed the MMR.
            val manifestPresent = scala.util
                .Try(DigestsVerifier.loadManifestFromDir(destDir))
                .isSuccess
            if !manifestPresent then {
                info("no digests.json in dest — downloading just the digests archive")
                val tip = meta.beacon.immutableFileNumber
                Await.result(
                  client.downloadCardanoDatabaseV2(
                    meta,
                    destDir,
                    immutableRange = MithrilClient.ImmutableFileRange.Range(tip, tip)
                  ),
                  10.minutes
                )
            }
            val manifest = DigestsVerifier.loadManifestAt(destDir)
            info(s"digests manifest entries: ${manifest.size}")

            // Use the same canonical-leaf logic the verifier does — sort by (immutable_number,
            // filename), filter by beacon's immutable_file_number cap, leaves are ASCII bytes of
            // hex digest strings.
            val cap = meta.beacon.immutableFileNumber
            val leaves = manifest.entries.iterator
                .flatMap { (name, digest) =>
                    val dot = name.indexOf('.')
                    if dot <= 0 then None
                    else
                        scala.util
                            .Try(name.substring(0, dot).toLong)
                            .toOption
                            .filter(_ <= cap)
                            .map(n => (n, name, digest))
                }
                .toSeq
                .sortBy { case (n, name, _) => (n, name) }
                .map { case (_, _, digest) =>
                    digest.getBytes(java.nio.charset.StandardCharsets.US_ASCII)
                }
            info(s"local MMR over ${leaves.size} digest leaves")

            val tStart = System.nanoTime()
            val localRootHex = MerkleMountainRange.computeRoot(leaves).toHex.toLowerCase
            val elapsed = (System.nanoTime() - tStart) / 1_000_000L
            info(s"local merkleRoot=$localRootHex (${elapsed}ms)")

            assert(
              localRootHex == meta.merkleRoot.toLowerCase,
              s"\nMMR root MISMATCH — our port disagrees with mithril-stm's MKTree:\n" +
                  s"  expected (aggregator) = ${meta.merkleRoot}\n" +
                  s"  recomputed (local)    = $localRootHex\n" +
                  s"  leaves                = ${leaves.size}\n" +
                  s"  beacon.immutable_file_number = $cap"
            )
            info("✓ MMR root matches — Scala port == mithril-stm MKTree")
        } finally client.close()
    }
}
