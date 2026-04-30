package scalus.cardano.node.stream

import scalus.uplc.builtin.ByteString

import java.nio.file.Path

/** Where to fetch a [[scalus.cardano.node.stream.engine.snapshot.ChainStoreSnapshot]] from at cold
  * start. The `ChainStoreRestorer` resolves each variant to an `InputStream`, drives the snapshot
  * reader, and bulk-loads the configured `ChainStore`.
  *
  * See `docs/local/claude/indexer/snapshot-bootstrap-m10.md` § *SnapshotSource + Restorer + startup
  * integration* for the cold-vs-warm startup rules.
  */
sealed trait SnapshotSource

object SnapshotSource {

    /** Local file on disk. The simplest source — bundled fixtures, internal snapshot rotation, CI
      * test setups.
      */
    case class File(path: Path) extends SnapshotSource

    /** HTTP(S) download. `expectedSha256`, when set, is compared against a SHA-256 computed over
      * the entire downloaded byte stream before the restorer trusts its content — lets third-party
      * mirrors serve the snapshot without the consumer trusting the transport. `None` relies on the
      * snapshot's internal footer hash alone (body integrity only, not header tampering).
      */
    case class Url(url: String, expectedSha256: Option[ByteString] = None) extends SnapshotSource

    /** Mithril aggregator source. Resolved at provider-construction time by an SPI implementation
      * loaded from the classpath (see [[engine.snapshot.MithrilSnapshotResolver]] — `META-INF`
      * service registration in `scalus-chain-store-mithril`). When the SPI is not on the
      * classpath, raises [[UnsupportedSourceException]].
      *
      * @param aggregatorUrl
      *   Mithril aggregator endpoint, e.g.
      *   `https://aggregator.testing-preview.api.mithril.network/aggregator`.
      * @param genesisVerificationKey
      *   hex-encoded genesis verification key for the target network.
      * @param workDir
      *   stable on-disk location for the downloaded V2 artefact. Must be a directory the resolver
      *   can write to; resumable across restarts via the `.extracted` markers the downloader
      *   leaves behind. Required (no default) because the artefact is multi-GB and a tmp-dir
      *   default would silently re-download on every start.
      * @param immutableFileRange
      *   `Some((from, to))` (inclusive 1-based) restricts which immutable chunks the resolver pulls;
      *   `None` means "all chunks the aggregator advertises". Useful when the CDN retains only a
      *   rolling window (e.g. testing-preview keeps ~15K most-recent chunks).
      * @param snapshotHash
      *   pin a specific signed snapshot by hash; `None` picks the aggregator's latest.
      */
    case class Mithril(
        aggregatorUrl: String,
        genesisVerificationKey: String,
        workDir: Path,
        immutableFileRange: Option[(Long, Long)] = None,
        snapshotHash: Option[String] = None
    ) extends SnapshotSource

    /** Already-extracted cardano-node snapshot directory — `<dir>/immutable/` and
      * `<dir>/ledger/<slot>/`. Skips the download + cryptographic verification entirely; useful
      * when the snapshot was acquired via an external pipeline (own signed bundle, mithril-client
      * CLI, CI fixture). Resolved by the same SPI as [[Mithril]] (the dir-restore path is the
      * common code).
      */
    case class MithrilDir(path: Path) extends SnapshotSource
}
