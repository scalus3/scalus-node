package scalus.cardano.node.stream.engine.snapshot.mithril

import cps.*
import cps.monads.FutureAsyncMonad
import scalus.cardano.node.stream.engine.snapshot.immutabledb.DigestsVerifier
import scalus.cardano.node.stream.engine.snapshot.{
    MithrilSnapshotResolver,
    SnapshotDirRestorer,
    SnapshotError
}
import scalus.cardano.node.stream.engine.{ChainStore, ChainStoreUtxoSet}
import scalus.cardano.node.stream.{ChainTip, SnapshotSource}

import java.nio.file.Files
import scala.concurrent.{ExecutionContext, Future}

/** ServiceLoader-discovered implementation of [[MithrilSnapshotResolver]].
  *
  * Two restore shapes:
  *
  *   - [[restore]] (Mithril aggregator) — instantiate the WASM-backed [[MithrilClient]], pick a
  *     snapshot (latest or pinned by hash), download the V2 artefact into the user-provided
  *     `workDir` (resumable across restarts via `.extracted` markers), then run
  *     [[SnapshotDirRestorer]] over the resulting on-disk layout.
  *   - [[restoreDir]] — skip download + cryptographic verification; just point
  *     [[SnapshotDirRestorer]] at the pre-extracted directory. Useful for trusted-fixture /
  *     external-pipeline cases where the caller takes responsibility for authenticity.
  *
  * The aggregator-fed [[restore]] path runs the full cryptographic chain:
  *
  *   1. WASM `verify_certificate_chain(certHash)` — walks the Mithril cert chain back to the
  *      genesis verification key (MuSig2 threshold-signature checks at each hop). Runs
  *      concurrently with the artefact download.
  *   2. [[DigestsVerifier]] — file-level SHA-256 cross-check against the manifest shipped in
  *      `digests.tar.zst`. Inline-computed digests from the just-finished extraction skip a
  *      full re-read of every immutable file.
  *   3. [[CardanoDatabaseVerifier]] — recompute the Cardano-Database Merkle root over the
  *      manifest entries and compare to the certificate's signed_message. A mismatch fails the
  *      whole restore — we never touch the store with an unauthenticated snapshot.
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
        val client = MithrilClient.create(source.aggregatorUrl, source.genesisVerificationKey)
        val task: Future[ChainTip] = async[Future] {
            val meta = await(pickSnapshotMeta(client, source.snapshotHash))
            Files.createDirectories(source.workDir)
            val range = source.immutableFileRange match {
                case None         => MithrilClient.ImmutableFileRange.Full
                case Some((a, b)) => MithrilClient.ImmutableFileRange.Range(a, b)
            }

            // Cert-chain verification runs concurrently with the download — independent work, and
            // the cert hash is known up-front.
            val certificateF = client.verifyCertificateChain(meta.certificateHash)
            val downloadF =
                client.downloadCardanoDatabaseV2(meta, source.workDir, immutableRange = range)
            val layout = await(downloadF)
            val certificate = await(certificateF)

            val manifest = DigestsVerifier.loadManifestAt(source.workDir)
            val fileLevel = DigestsVerifier.verifyWithCache(
              source.workDir.resolve("immutable"),
              manifest,
              layout.inlineDigests
            )
            if !fileLevel.presentMatchesManifest then
                throw SnapshotError.SnapshotCorrupted(
                  s"file-level digest mismatch: ${fileLevel.mismatches.size} bad files; first: " +
                      fileLevel.mismatches.headOption.map(_.fileName).getOrElse("<none>")
                )
            CardanoDatabaseVerifier.verify(certificate, manifest, meta.beacon.immutableFileNumber)

            runDirRestore(source.workDir, store)
        }
        task.andThen { case _ => client.close() }
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
    )(using ExecutionContext): Future[MithrilMessages.CardanoDatabaseV2Metadata] =
        async[Future] {
            pinnedHash match {
                case Some(hash) =>
                    await(client.getCardanoDatabaseV2Snapshot(hash)).getOrElse(
                      throw SnapshotError.SnapshotConfigError(
                        s"aggregator returned no snapshot for pinned hash=$hash"
                      )
                    )
                case None =>
                    val list = await(client.listCardanoDatabaseV2Snapshots())
                    val latest = list.headOption.getOrElse(
                      throw SnapshotError.SnapshotConfigError(
                        "aggregator returned no Cardano Database V2 snapshots"
                      )
                    )
                    await(client.getCardanoDatabaseV2Snapshot(latest.hash)).getOrElse(
                      throw SnapshotError.SnapshotConfigError(
                        s"aggregator listed snapshot ${latest.hash} but full metadata is missing"
                      )
                    )
            }
        }
}
