package scalus.cardano.node.stream.engine.snapshot.mithril

import scalus.cardano.node.stream.engine.snapshot.immutabledb.DigestsVerifier
import scalus.utils.Hex.toHex

/** End-to-end cryptographic verifier for a downloaded Mithril Cardano-Database V2 artefact.
  *
  * Given:
  *
  *   - a `MithrilCertificateMessage` already returned by [[MithrilClient.verifyCertificateChain]]
  *     (i.e. the Mithril cert chain has already been walked back to the genesis verification key by
  *     the WASM client);
  *   - the snapshot's digests manifest as loaded by [[DigestsVerifier]];
  *   - the snapshot's `beacon.immutableFileNumber` (the cap on which immutable-trio entries were
  *     signed for);
  *
  * we compute the local Cardano-Database Merkle root over the per-file digests and check that, once
  * substituted into the certificate's `protocol_message`, the recomputed `signed_message` matches
  * the certificate's signed_message verbatim. A match proves the snapshot's contents are exactly
  * what the aggregator's threshold signature attests to — cryptographic authenticity, not just
  * file-level integrity.
  *
  * On failure we surface a typed [[VerificationError]] so the resolver can refuse to touch the
  * store with an unauthenticated snapshot.
  */
object CardanoDatabaseVerifier {

    sealed trait VerificationError extends RuntimeException
    object VerificationError {
        final case class MerkleRootMismatch(expectedSigned: String, computedSigned: String)
            extends RuntimeException(
              s"Cardano-Database Merkle root does not match certificate signed_message. " +
                  s"Expected $expectedSigned, recomputed $computedSigned. The downloaded snapshot " +
                  "is NOT what the aggregator signed."
            )
            with VerificationError

        final case class EmptyManifest(detail: String)
            extends RuntimeException(s"empty/insufficient digests manifest: $detail")
            with VerificationError
    }

    /** Verify the snapshot. Returns `Unit` on success; throws a [[VerificationError]] on any
      * failure mode. The intended call site is inside the resolver's `Future` body so the failure
      * surfaces back to the bootstrap caller as a Future failure.
      *
      * @param certificate
      *   already-verified Mithril certificate (the WASM client did the chain walk)
      * @param manifest
      *   digests manifest loaded from `digests.tar.zst`
      * @param beaconImmutableFileNumber
      *   `snapshot.beacon.immutable_file_number` — the upper bound (inclusive) on signed entries
      */
    def verify(
        certificate: MithrilMessages.MithrilCertificateMessage,
        manifest: DigestsVerifier.DigestManifest,
        beaconImmutableFileNumber: Long
    ): Unit = {
        val digestLeaves = canonicalDigestLeaves(manifest, beaconImmutableFileNumber)
        if digestLeaves.isEmpty then
            throw VerificationError.EmptyManifest(
              s"digests manifest had no entries with immutable_file_number ≤ " +
                  s"$beaconImmutableFileNumber"
            )

        val rootBytes = MerkleMountainRange.computeRoot(
          digestLeaves.map(_._2.getBytes(java.nio.charset.StandardCharsets.US_ASCII))
        )
        val rootHex = rootBytes.toHex.toLowerCase

        val partsWithRoot = certificate.protocolMessage.messageParts.updated(
          ProtocolMessagePartKey.CardanoDatabaseMerkleRoot.snakeCase,
          rootHex
        )
        val recomputed = ProtocolMessageHash.compute(partsWithRoot)

        if recomputed != certificate.signedMessage then
            throw VerificationError.MerkleRootMismatch(certificate.signedMessage, recomputed)
    }

    /** Filter the manifest to entries whose immutable-file-number is ≤ `cap`, then sort by
      * `(number, filename)` to match upstream's `BTreeMap<ImmutableFile, _>` iteration order (which
      * is `(number, full-path)` — within a single number the path differs only in the extension, so
      * filename-lex among `chunk` / `primary` / `secondary` is the right tiebreaker). Returns
      * `(filename, digestHex)` pairs.
      *
      * Sorting by parsed number is load-bearing once immutable numbers exceed the 5-digit
      * zero-padded width: filename-lex would put `"100000.chunk"` before `"99999.chunk"`, breaking
      * the Merkle root.
      */
    private def canonicalDigestLeaves(
        manifest: DigestsVerifier.DigestManifest,
        cap: Long
    ): Seq[(String, String)] =
        manifest.entries.iterator
            .flatMap { (name, digest) =>
                extractImmutableNumber(name).filter(_ <= cap).map(n => (n, name, digest))
            }
            .toSeq
            .sortBy { case (n, name, _) => (n, name) }
            .map { case (_, name, digest) => (name, digest) }

    /** Parse the leading numeric prefix of an immutable filename like `00042.chunk`. Returns `None`
      * for unrecognised names — defensive against future manifest entries that aren't trio members.
      */
    private def extractImmutableNumber(filename: String): Option[Long] = {
        val dot = filename.indexOf('.')
        if dot <= 0 then None
        else
            try Some(filename.substring(0, dot).toLong)
            catch case _: NumberFormatException => None
    }
}
