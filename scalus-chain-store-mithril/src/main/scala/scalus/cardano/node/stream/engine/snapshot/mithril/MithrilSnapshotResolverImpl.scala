package scalus.cardano.node.stream.engine.snapshot.mithril

import scalus.cardano.node.stream.engine.snapshot.{
    MithrilSnapshotResolver,
    SnapshotDirRestorer,
    SnapshotError
}
import scalus.cardano.node.stream.engine.{ChainStore, ChainStoreUtxoSet}
import scalus.cardano.node.stream.{ChainTip, SnapshotSource}

import java.nio.file.Files
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** ServiceLoader-discovered implementation of [[MithrilSnapshotResolver]].
  *
  * Two restore shapes:
  *
  *   - [[restore]] (Mithril aggregator) — instantiate the WASM-backed [[MithrilClient]], pick a
  *     snapshot (latest or pinned by hash), download the V2 artefact into the user-provided
  *     `workDir` (resumable across restarts via `.extracted` markers), then run
  *     [[SnapshotDirRestorer]] over the resulting on-disk layout.
  *   - [[restoreDir]] — skip download + verification; just point [[SnapshotDirRestorer]] at the
  *     pre-extracted directory. Useful for trusted-fixture / external-pipeline cases.
  *
  * Cryptographic verification (cert-chain walk + Merkle anchoring) is not yet wired — currently
  * relies on the digests-manifest cross-check inside `DigestsVerifier` for file-level integrity.
  * See `docs/local/claude/indexer/snapshot-bootstrap-m10.md` *Other deferred work* for the path
  * to anchor the manifest into the signed Mithril chain via `verify_certificate_chain`.
  */
final class MithrilSnapshotResolverImpl extends MithrilSnapshotResolver {

    def restore(
        source: SnapshotSource.Mithril,
        store: ChainStore & ChainStoreUtxoSet
    )(using ExecutionContext): Future[ChainTip] = {
        // Validate immutableFileRange before any I/O so that config errors surface as
        // SnapshotConfigError rather than as an IllegalArgumentException from ResolvedRange.
        source.immutableFileRange match {
            case Some((a, b)) if a < 1 || b < a =>
                return Future.failed(
                  SnapshotError.SnapshotConfigError(
                    s"immutableFileRange ($a, $b) is invalid: bounds must satisfy from >= 1 and from <= to"
                  )
                )
            case _ =>
        }
        Future {
            val client = MithrilClient.create(source.aggregatorUrl, source.genesisVerificationKey)
            try {
                val meta = pickSnapshotMeta(client, source.snapshotHash)
                Files.createDirectories(source.workDir)
                val range = source.immutableFileRange match {
                    case None         => MithrilClient.ImmutableFileRange.Full
                    case Some((a, b)) => MithrilClient.ImmutableFileRange.Range(a, b)
                }
                // Bounded blocking — the download is multi-GB and may take minutes; the resolver runs
                // on the user-supplied EC, and the caller (ChainStoreRestorer) already returns a
                // Future, so blocking inside this Future body is fine.
                Await.result(
                    client.downloadCardanoDatabaseV2(meta, source.workDir, immutableRange = range),
                    Duration.Inf
                )
                runDirRestore(source.workDir, store)
            } finally client.close()
        }
    }

    def restoreDir(
        source: SnapshotSource.MithrilDir,
        store: ChainStore & ChainStoreUtxoSet
    )(using ExecutionContext): Future[ChainTip] = Future {
        if !Files.isDirectory(source.path) then
            throw SnapshotError.SnapshotConfigError(
              s"SnapshotSource.MithrilDir path is not a directory: ${source.path}"
            )
        runDirRestore(source.path, store)
    }

    private def runDirRestore(
        dir: java.nio.file.Path,
        store: ChainStore & ChainStoreUtxoSet
    ): ChainTip = {
        val stats = new SnapshotDirRestorer(store).restore(dir)
        stats.blocks.tip.getOrElse(
          throw SnapshotError.SnapshotCorrupted(
            s"SnapshotDirRestorer at $dir produced no tip — restore yielded zero blocks"
          )
        )
    }

    private def pickSnapshotMeta(
        client: MithrilClient,
        pinnedHash: Option[String]
    )(using ExecutionContext): MithrilMessages.CardanoDatabaseV2Metadata = pinnedHash match {
        case Some(hash) =>
            Await.result(client.getCardanoDatabaseV2Snapshot(hash), Duration.Inf).getOrElse(
              throw SnapshotError.SnapshotConfigError(
                s"aggregator returned no snapshot for pinned hash=$hash"
              )
            )
        case None =>
            val list = Await.result(client.listCardanoDatabaseV2Snapshots(), Duration.Inf)
            val latest = list.headOption.getOrElse(
              throw SnapshotError.SnapshotConfigError(
                "aggregator returned no Cardano Database V2 snapshots"
              )
            )
            Await.result(client.getCardanoDatabaseV2Snapshot(latest.hash), Duration.Inf).getOrElse(
              throw SnapshotError.SnapshotConfigError(
                s"aggregator listed snapshot ${latest.hash} but full metadata is missing"
              )
            )
    }
}
