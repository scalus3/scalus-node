package scalus.cardano.node.stream

/** Selects the cryptographic verification path for [[SnapshotSource.Mithril]].
  *
  * The default is [[Wasm]] — full cert-chain authentication via the embedded Mithril WASM client.
  * The other two modes trade verification work for restore latency or ship-readiness, and exist
  * because chain-walk performance under Chicory's interpreter is on the order of tens of minutes on
  * preview (interpreted blst pairings — see `docs/local/design/scala-only-verification-m10e.md`).
  */
sealed trait MithrilVerificationMode

object MithrilVerificationMode {

    /** Skip cryptographic chain verification. The file-level SHA-256 cross-check against the
      * digests manifest still runs, so corruption-on-disk and truncation are caught — but the
      * manifest itself isn't anchored against the threshold-signed certificate, so a hostile
      * aggregator could ship a self-consistent fabrication and we'd accept it.
      *
      * **Not a security claim.** Use only when the snapshot is already trusted via another channel
      * (CI fixture, internal mirror with separate signing, debugging) or when restore latency
      * outweighs the authenticity guarantee for the situation.
      */
    case object SkipVerification extends MithrilVerificationMode

    /** Default. Full upstream verification: walks the certificate chain via the embedded WASM
      * Mithril client back to the genesis verification key (MuSig2 threshold signatures at each
      * hop), then anchors the locally-computed Cardano-Database Merkle root in the verified cert.
      * Cryptographically authentic; minutes-scale on preview because the Chicory interpreter runs
      * the blst pairings.
      */
    case object Wasm extends MithrilVerificationMode

    /** Cross-platform Scala-native chain verifier. Same authenticity guarantee as [[Wasm]] but uses
      * scalus's BLS façade (native `supranational.blst` on JVM, `@noble/curves` on JS) instead of
      * the embedded WASM client — eliminating the interpreter overhead. Not yet implemented; see
      * `docs/local/design/scala-only-verification-m10e.md` for the M10e plan. Selecting this mode
      * today raises [[UnsupportedSourceException]] at restore time.
      */
    case object ScalaOnly extends MithrilVerificationMode
}
