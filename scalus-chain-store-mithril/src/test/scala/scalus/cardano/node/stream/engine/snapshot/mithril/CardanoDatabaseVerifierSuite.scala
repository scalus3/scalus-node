package scalus.cardano.node.stream.engine.snapshot.mithril

import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.node.stream.engine.snapshot.immutabledb.DigestsVerifier
import scalus.utils.Hex.toHex

/** Round-trip the verifier with a hand-constructed certificate. The point is to prove the
  * orchestration glue (manifest filtering → MMR root → protocol-message hash → comparison) is
  * self-consistent. End-to-end validation against a real aggregator-signed snapshot remains a
  * separate (env-gated) probe; this suite stays offline.
  */
final class CardanoDatabaseVerifierSuite extends AnyFunSuite {

    private def makeManifest(entries: Seq[(String, String)]): DigestsVerifier.DigestManifest =
        DigestsVerifier.DigestManifest(entries.toMap)

    private val sampleEntries: Seq[(String, String)] = Seq(
      "00000.chunk" -> "aaaa",
      "00000.primary" -> "bbbb",
      "00000.secondary" -> "cccc",
      "00001.chunk" -> "dddd",
      "00001.primary" -> "eeee",
      "00001.secondary" -> "ffff"
    )

    private def buildAuthenticCert(
        parts: Map[String, String]
    ): MithrilMessages.MithrilCertificateMessage =
        MithrilMessages.MithrilCertificateMessage(
          hash = "cert-h",
          previousHash = "prev-h",
          protocolMessage = MithrilMessages.ProtocolMessage(parts),
          signedMessage = ProtocolMessageHash.compute(parts),
          aggregateVerificationKey = "avk"
        )

    test("verify succeeds when local Merkle root matches the certificate's signed_message") {
        val manifest = makeManifest(sampleEntries)
        val expectedLeaves =
            sampleEntries.sortBy(_._1).map(_._2.getBytes(java.nio.charset.StandardCharsets.US_ASCII))
        val expectedRootHex =
            MerkleMountainRange.computeRoot(expectedLeaves).toHex.toLowerCase

        val parts: Map[String, String] = Map(
          "current_epoch" -> "42",
          "latest_block_number" -> "100",
          "cardano_database_merkle_root" -> expectedRootHex
        )
        val cert = buildAuthenticCert(parts)

        CardanoDatabaseVerifier.verify(cert, manifest, beaconImmutableFileNumber = 1L)
    }

    test("verify rejects a certificate with a stale signed_message (Merkle root mismatch)") {
        val manifest = makeManifest(sampleEntries)
        val parts: Map[String, String] = Map(
          "current_epoch" -> "42",
          "cardano_database_merkle_root" -> "00" * 32
        )
        val cert = buildAuthenticCert(parts)
        val ex = intercept[CardanoDatabaseVerifier.VerificationError.MerkleRootMismatch](
          CardanoDatabaseVerifier.verify(cert, manifest, beaconImmutableFileNumber = 1L)
        )
        assert(ex.getMessage.contains("does not match certificate signed_message"))
    }

    test("verify filters entries past the beacon's immutable_file_number") {
        val manifest = makeManifest(sampleEntries)
        val cappedLeaves = sampleEntries
            .filter(_._1.startsWith("00000."))
            .sortBy(_._1)
            .map(_._2.getBytes(java.nio.charset.StandardCharsets.US_ASCII))
        val expectedRootHex =
            MerkleMountainRange.computeRoot(cappedLeaves).toHex.toLowerCase
        val parts = Map(
          "cardano_database_merkle_root" -> expectedRootHex
        )
        val cert = buildAuthenticCert(parts)
        CardanoDatabaseVerifier.verify(cert, manifest, beaconImmutableFileNumber = 0L)
    }

    test("verify rejects an empty (post-filter) manifest with EmptyManifest") {
        val manifest = makeManifest(sampleEntries)
        val cert = buildAuthenticCert(Map("cardano_database_merkle_root" -> "00" * 32))
        val ex = intercept[CardanoDatabaseVerifier.VerificationError.EmptyManifest](
          CardanoDatabaseVerifier.verify(cert, manifest, beaconImmutableFileNumber = -1L)
        )
        assert(ex.getMessage.contains("immutable_file_number"))
    }

    test("ordering uses parsed numeric prefix, not filename-lex (handles 6+ digit numbers)") {
        // Once immutable numbers exceed 5 digits, filename-lex would put "100000.chunk" BEFORE
        // "99999.chunk" — wrong order. The verifier must sort by (number, filename).
        val mixed = Seq(
          "099999.chunk" -> "aa",
          "099999.primary" -> "bb",
          "099999.secondary" -> "cc",
          "100000.chunk" -> "dd",
          "100000.primary" -> "ee",
          "100000.secondary" -> "ff"
        )
        val manifest = makeManifest(mixed)
        // Numeric ordering — 99999 then 100000.
        val numericOrder = Seq("aa", "bb", "cc", "dd", "ee", "ff")
            .map(_.getBytes(java.nio.charset.StandardCharsets.US_ASCII))
        val numericRoot =
            MerkleMountainRange.computeRoot(numericOrder).toHex.toLowerCase
        val cert = buildAuthenticCert(Map("cardano_database_merkle_root" -> numericRoot))
        // If the verifier sorted by filename-lex it would compute over [dd, ee, ff, aa, bb, cc]
        // instead, producing a different root and failing the comparison.
        CardanoDatabaseVerifier.verify(cert, manifest, beaconImmutableFileNumber = 100000L)
    }
}
