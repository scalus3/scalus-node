package scalus.cardano.node.stream.engine.snapshot.mithril

import com.github.luben.zstd.ZstdInputStream
import com.github.plokhotnyuk.jsoniter_scala.core.{readFromString, JsonValueCodec}
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import scalus.utils.Hex.toHex

import java.io.InputStream
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Path, StandardCopyOption}
import java.security.{DigestInputStream, MessageDigest}
import java.time.Duration
import java.util.concurrent.Semaphore
import scala.concurrent.{ExecutionContext, Future}

/** User-facing entry point for the Mithril-verified snapshot acquisition path.
  *
  * Splits responsibilities between the embedded WASM client and the JVM:
  *
  *   - **WASM**: cryptographic verification — certificate chain walk + MuSig2 threshold signature.
  *     Drives through `mithrilclient_new` + `verify_certificate_chain` on the pinned
  *     `@mithril-dev/mithril-client-wasm@0.10.4` blob. Metadata listing (`list_certificates`,
  *     `list_cardano_database_snapshots`, `get_cardano_database_v2_snapshot`) also routes through
  *     the WASM so the caller can demonstrate they're talking to the same aggregator the verifier
  *     will check against, though the JSON wire shape is decoded on our side.
  *   - **JVM**: HTTP file transport (`immutable-{N}.tar.zst`, `ancillary.tar.zst`,
  *     `digests.tar.zst`) via `java.net.http.HttpClient`, plus zstd + tar extraction via `zstd-jni`
  *     + `commons-compress`. The WASM doesn't compile the upstream `fs` feature (and
  *     `wasm32-unknown-unknown` has no filesystem anyway), so we do this half directly.
  *
  * All WASM calls serialise onto `MithrilAsyncRuntime`'s single dispatcher thread. HTTP calls run
  * on the supplied `ExecutionContext`.
  *
  * {{{
  * val client = MithrilClient.create(
  *     aggregatorUrl = "https://aggregator.testing-preview.api.mithril.network/aggregator",
  *     genesisVerificationKey = "5b3132372c37332c…"
  * )
  * try {
  *     val latest = Await.result(client.listCardanoDatabaseV2Snapshots(), 30.seconds).head
  *     val meta = Await.result(client.getCardanoDatabaseV2Snapshot(latest.hash), 30.seconds).get
  *     val files = Await.result(client.downloadImmutable(meta, latest.beacon.immutableFileNumber, workDir), 60.seconds)
  * } finally client.close()
  * }}}
  */
final class MithrilClient private (
    private val runtime: MithrilWasmRuntime,
    private val asyncRuntime: MithrilAsyncRuntime,
    private val clientPtr: Long,
    private val httpClient: HttpClient
)(using ec: ExecutionContext) {

    import MithrilMessages.*

    /** `mithrilclient_list_mithril_certificates` — latest-first. */
    def listCertificates(): Future[Seq[MithrilCertificateListItem]] =
        callAsyncJson[Seq[MithrilCertificateListItem]]("mithrilclient_list_mithril_certificates")

    /** `mithrilclient_list_cardano_database_v2` — latest-first. */
    def listCardanoDatabaseV2Snapshots(): Future[Seq[CardanoDatabaseV2ListItem]] =
        callAsyncJson[Seq[CardanoDatabaseV2ListItem]]("mithrilclient_list_cardano_database_v2")

    /** `mithrilclient_get_cardano_database_v2` — full metadata including download `locations`. */
    def getCardanoDatabaseV2Snapshot(hash: String): Future[Option[CardanoDatabaseV2Metadata]] =
        withHashArg("mithrilclient_get_cardano_database_v2", hash)
            .flatMap(asyncRuntime.awaitPromise(_)(optionalJsonDecode[CardanoDatabaseV2Metadata]))

    /** `mithrilclient_verify_certificate_chain` — walks the certificate chain back to the genesis
      * verification key (MuSig2 threshold-signature checks at each hop) and returns the verified
      * leaf certificate. A failed walk surfaces as a Future failure carrying the WASM error string;
      * no certificate is returned in that case.
      *
      * The returned certificate's `signedMessage` is a SHA-256 of its
      * `protocolMessage.messageParts` computed via [[ProtocolMessageHash]] — re-deriving it locally
      * with our own merkle root in place of the aggregator's is what proves snapshot authenticity
      * (see [[CardanoDatabaseVerifier]]).
      */
    def verifyCertificateChain(hash: String): Future[MithrilCertificateMessage] =
        withHashArg("mithrilclient_verify_certificate_chain", hash)
            .flatMap(asyncRuntime.awaitPromise(_)(jsonDecode[MithrilCertificateMessage]))

    /** Download one `immutable-{N}.tar.zst` file named by `meta.immutables.locations`, expand it
      * under `destDir` (which must exist), and return the paths of extracted files. Currently picks
      * the first `CloudStorage` location with a non-empty `Template` URI.
      */
    def downloadImmutable(
        meta: CardanoDatabaseV2Metadata,
        immutableFileNumber: Long,
        destDir: Path
    ): Future[Seq[Path]] = Future {
        val url = immutableUrl(meta, immutableFileNumber)
        httpGetStream(url) { in => extractTarZst(in, destDir).paths }
    }

    /** Bulk-download the full Cardano Database V2 artifact referenced by `meta`: every
      * `immutable-{1..N}.tar.zst` chunk, plus `ancillary.tar.zst` (ledger state + the last
      * immutable) and `digests.tar.zst` (per-file hashes for later Merkle verification). Each
      * archive is streamed directly from HTTP into the zstd+tar extractor — no multi-GB byte arrays
      * are held in memory.
      *
      * Layout on disk mirrors cardano-node's: `destDir/immutable/{n}.chunk|primary|secondary`,
      * `destDir/ledger/...` (from ancillary), plus whatever the digests archive contains.
      *
      * **Resumability.** An archive is skipped when a sibling marker file
      * `<name>.tar.zst.extracted` already exists next to the extracted output — we drop the marker
      * only after extraction finishes cleanly, so a crash mid-extract forces a fresh re-download on
      * the next run.
      *
      * **Concurrency.** Up to `maxConcurrent` archives are downloaded in parallel on the supplied
      * `ExecutionContext`. Per-archive IO remains sequential (one HTTP connection, one zstd+tar
      * stream). Default 4 matches what `mithril-client` uses as its soft default.
      *
      * **Progress.** `onProgress` fires once per archive as it completes, with the archive's stage
      * label and byte counts. Byte counts reflect what the server advertised in
      * `meta.{immutables.averageSizeUncompressed, ancillary.sizeUncompressed, digests.sizeUncompressed}`
      * rather than metered HTTP bytes — cheap to surface, good enough for UX.
      *
      * No Merkle root verification yet — that lands with the cert-chain walk in a follow-up.
      */
    def downloadCardanoDatabaseV2(
        meta: CardanoDatabaseV2Metadata,
        destDir: Path,
        onProgress: MithrilClient.DownloadProgress => Unit = _ => (),
        maxConcurrent: Int = MithrilClient.DefaultMaxConcurrent,
        immutableRange: MithrilClient.ImmutableFileRange = MithrilClient.ImmutableFileRange.Full
    ): Future[MithrilClient.CardanoDatabaseV2Layout] = {
        require(maxConcurrent >= 1, s"maxConcurrent must be >= 1, got $maxConcurrent")
        Files.createDirectories(destDir)
        val total = meta.beacon.immutableFileNumber
        require(total >= 1, s"beacon.immutableFileNumber must be >= 1, got $total")
        val range = immutableRange.resolve(total)

        val ancillaryUrl0 = meta.ancillary.locations.collectFirst {
            case AncillaryLocation.CloudStorage(uri, _) => uri
        }
        val digestsUrl0 = meta.digests.locations.collectFirst {
            case DigestLocation.CloudStorage(uri, _) => uri
            case DigestLocation.Aggregator(uri)      => uri
        }

        val immutableJobs = (range.start to range.end).map { n =>
            MithrilClient.ArchiveJob(
              stage = s"immutable-$n",
              url = immutableUrl(meta, n),
              advertisedSize = Some(meta.immutables.averageSizeUncompressed)
            )
        }
        val ancillaryJob = ancillaryUrl0.map { u =>
            MithrilClient.ArchiveJob("ancillary", u, Some(meta.ancillary.sizeUncompressed))
        }
        val digestsJob = digestsUrl0.map { u =>
            MithrilClient.ArchiveJob("digests", u, Some(meta.digests.sizeUncompressed))
        }
        val allJobs = (immutableJobs ++ ancillaryJob ++ digestsJob).toVector

        val sem = new Semaphore(maxConcurrent)
        val perJob = allJobs.map { job =>
            Future {
                sem.acquire()
                try runArchiveJob(job, destDir, onProgress)
                finally sem.release()
            }
        }
        Future.sequence(perJob).map { extracted =>
            val byStage = allJobs.zip(extracted).toMap
            // Inline-computed digests across every fresh-extracted file. Resumed archives (those
            // whose .extracted marker fired) contribute nothing to this map; the verifier falls
            // back to a disk read for those filenames.
            val inline = extracted.foldLeft(Map.empty[String, String])(_ ++ _.digests)
            MithrilClient.CardanoDatabaseV2Layout(
              root = destDir,
              immutableRange = (range.start, range.end),
              immutableFiles = immutableJobs.flatMap(byStage(_).paths),
              ancillaryFiles = ancillaryJob.map(byStage(_).paths).getOrElse(Seq.empty),
              digestsFiles = digestsJob.map(byStage(_).paths).getOrElse(Seq.empty),
              inlineDigests = inline
            )
        }
    }

    /** Release the underlying WASM runtime + dispatcher. Subsequent calls will fail. */
    def close(): Unit = asyncRuntime.close()

    // -------- internals --------

    private def callAsyncJson[T: JsonValueCodec](exportName: String): Future[T] =
        asyncRuntime
            .submit { _ => runtime.exportFn(exportName).apply(clientPtr)(0).toInt }
            .flatMap(asyncRuntime.awaitPromise(_)(jsonDecode[T]))

    private def withHashArg(exportName: String, hash: String): Future[Int] =
        asyncRuntime.submit { _ =>
            val (ptr, len) = runtime.passString(hash)
            runtime
                .exportFn(exportName)
                .apply(clientPtr, ptr.toLong, len.toLong)(0)
                .toInt
        }

    private def jsonDecode[T: JsonValueCodec](v: AnyRef | Null): T = {
        val json = WbindgenAbi.jsonStringify(v)
        readFromString[T](json)
    }

    private def optionalJsonDecode[T: JsonValueCodec](v: AnyRef | Null): Option[T] = v match {
        case null                  => None
        case WbindgenAbi.Undefined => None
        case other                 => Some(jsonDecode[T](other))
    }

    private def immutableUrl(meta: CardanoDatabaseV2Metadata, n: Long): String =
        MithrilClient.immutableUrl(meta, n)

    /** Download one archive (idempotent via sibling `.extracted` marker), stream-extract it into
      * `destDir`, fire `onProgress` with the advertised size, return the extracted paths. Caller
      * holds the concurrency permit.
      */
    private def runArchiveJob(
        job: MithrilClient.ArchiveJob,
        destDir: Path,
        onProgress: MithrilClient.DownloadProgress => Unit
    ): MithrilClient.ExtractedFiles = {
        val markerName = job.stage + ".extracted"
        val marker = destDir.resolve(markerName)
        if Files.exists(marker) then {
            onProgress(
              MithrilClient.DownloadProgress(job.stage, 0L, job.advertisedSize, skipped = true)
            )
            return MithrilClient.ExtractedFiles.empty
        }
        val extracted = httpGetStream(job.url) { in => extractTarZst(in, destDir) }
        Files.writeString(marker, job.url)
        onProgress(
          MithrilClient.DownloadProgress(
            job.stage,
            job.advertisedSize.getOrElse(0L),
            job.advertisedSize,
            skipped = false
          )
        )
        extracted
    }

    private def httpGetStream[T](url: String)(f: InputStream => T): T = {
        val req = HttpRequest
            .newBuilder(URI.create(url))
            .timeout(Duration.ofSeconds(MithrilClient.DownloadTimeoutSeconds))
            .GET()
            .build()
        val resp = httpClient.send(req, HttpResponse.BodyHandlers.ofInputStream())
        if resp.statusCode != 200 then {
            // Drain up to 4 KB of the body so the caller sees *why* — a 403 from the CDN
            // typically carries an XML <Error><Code>…</Code><Message>…</Message></Error> payload,
            // and a plain `→ 403` is almost useless without it. Cap the read so we don't slurp a
            // multi-MB HTML error page into the exception string.
            val snippet = scala.util
                .Try {
                    val buf = resp.body.readNBytes(4096)
                    new String(buf, java.nio.charset.StandardCharsets.UTF_8)
                }
                .getOrElse("<unavailable>")
            scala.util.Try(resp.body.close())
            throw new RuntimeException(
              s"GET $url → ${resp.statusCode}; body: ${snippet.take(4096)}"
            )
        }
        val body = resp.body
        try f(body)
        finally body.close()
    }

    private def extractTarZst(in: InputStream, destDir: Path): MithrilClient.ExtractedFiles = {
        Files.createDirectories(destDir)
        val written = scala.collection.mutable.ArrayBuffer.empty[Path]
        val digests = scala.collection.mutable.Map.empty[String, String]
        val zin = new ZstdInputStream(in)
        val tin = new TarArchiveInputStream(zin)
        // One digest instance reused across entries via reset() — saves per-entry allocation.
        // The DigestInputStream wraps `tin` so reads through it (via Files.copy) accumulate the
        // digest of the bytes Files.copy actually consumed for this entry.
        val digest = MessageDigest.getInstance("SHA-256")
        val digestIn = new DigestInputStream(tin, digest)
        try {
            var e = tin.getNextEntry
            while e != null do {
                if !e.isDirectory then {
                    val rel = sanitiseTarName(e.getName)
                    val out = destDir.resolve(rel)
                    Files.createDirectories(out.getParent)
                    digest.reset()
                    Files.copy(digestIn, out, StandardCopyOption.REPLACE_EXISTING)
                    digests.update(out.getFileName.toString, digest.digest().toHex)
                    written += out
                }
                e = tin.getNextEntry
            }
        } finally {
            tin.close()
            zin.close()
        }
        MithrilClient.ExtractedFiles(written.toSeq, digests.toMap)
    }

    /** Reject absolute / parent-escape entries — a cardano-node immutable tar should only ever
      * contain `immutable/NNNN.*` paths, but a hostile aggregator could ship `../../etc/passwd`.
      * Strip absolute prefixes and bail on `..` components.
      */
    private def sanitiseTarName(name: String): String = {
        val p = java.nio.file.Paths.get(name).normalize()
        require(!p.isAbsolute && !p.startsWith(".."), s"unsafe tar entry: $name")
        p.toString
    }
}

object MithrilClient {

    /** Timeout for per-file snapshot downloads. An immutable chunk is ~500 KB compressed; the
      * ancillary tar can approach 1 GB on mainnet and may need a higher ceiling.
      */
    val DownloadTimeoutSeconds: Long = 600L

    /** Default parallelism for [[MithrilClient.downloadCardanoDatabaseV2]] — matches upstream
      * `mithril-client`'s default; bounded so we don't saturate the CDN or the host's file handles.
      */
    val DefaultMaxConcurrent: Int = 4

    /** Progress record emitted by [[MithrilClient.downloadCardanoDatabaseV2]] as each archive
      * finishes (or is skipped because a prior run's marker is present).
      *
      * @param stage
      *   `"immutable-N"`, `"ancillary"`, or `"digests"` — stable enough to key resumability
      *   markers.
      * @param bytesDownloaded
      *   advertised-size bytes attributed to this archive (0 if skipped).
      * @param totalExpected
      *   the aggregator's announced size, when available.
      * @param skipped
      *   `true` when the `.extracted` marker shortcut fired.
      */
    final case class DownloadProgress(
        stage: String,
        bytesDownloaded: Long,
        totalExpected: Option[Long],
        skipped: Boolean
    )

    /** Output of [[MithrilClient.downloadCardanoDatabaseV2]] — enumerates what was extracted so
      * downstream parsers can iterate without scanning the directory.
      *
      * Note: when a previous run already extracted an archive, the corresponding paths are
      * **omitted** from these lists — the `.extracted` marker skips re-materialisation and we don't
      * pay the cost of re-walking the tar just to rebuild the path list. Callers that need the full
      * on-disk inventory should scan `root` directly.
      *
      * @param inlineDigests
      *   `filename → SHA-256 hex` for every file extracted in *this* run. Resumed archives
      *   contribute nothing — verifiers must fall back to a disk read for filenames missing from
      *   this map. See [[scalus.cardano.node.stream.engine.snapshot.immutabledb.DigestsVerifier]].
      */
    final case class CardanoDatabaseV2Layout(
        root: Path,
        immutableRange: (Long, Long),
        immutableFiles: Seq[Path],
        ancillaryFiles: Seq[Path],
        digestsFiles: Seq[Path],
        inlineDigests: Map[String, String] = Map.empty
    ) {
        def immutableCount: Long = immutableRange._2 - immutableRange._1 + 1
    }

    /** Per-archive extraction result — paths plus inline-computed SHA-256 digests, keyed by
      * filename (not full path). The filename matches what the digests manifest uses, so the
      * verifier can look these up directly without a disk re-read.
      */
    final case class ExtractedFiles(paths: Seq[Path], digests: Map[String, String])

    object ExtractedFiles {
        val empty: ExtractedFiles = ExtractedFiles(Seq.empty, Map.empty)
    }

    /** Selects which immutable chunks of a snapshot to pull. Mirrors upstream Mithril's
      * `ImmutableFileRange`. Indices are inclusive and 1-based to match cardano-node's immutable
      * file numbering. `Full` is the "download everything" default; the others are needed when a
      * CDN retains only a rolling window of recent immutables (as `testing-preview` does), or for
      * test fixtures / incremental restores.
      */
    sealed trait ImmutableFileRange {
        def resolve(last: Long): ResolvedRange = this match {
            case ImmutableFileRange.Full        => ResolvedRange(1L, last)
            case ImmutableFileRange.From(from)  => ResolvedRange(from, last)
            case ImmutableFileRange.UpTo(to)    => ResolvedRange(1L, to)
            case ImmutableFileRange.Range(a, b) => ResolvedRange(a, b)
        }
    }
    object ImmutableFileRange {
        case object Full extends ImmutableFileRange
        final case class From(from: Long) extends ImmutableFileRange
        final case class UpTo(to: Long) extends ImmutableFileRange
        final case class Range(from: Long, to: Long) extends ImmutableFileRange
    }

    /** Internal inclusive-bounds range descriptor returned by `ImmutableFileRange.resolve`. */
    final case class ResolvedRange(start: Long, end: Long) {
        require(start >= 1L && end >= start, s"invalid immutable range [$start..$end]")
    }

    private final case class ArchiveJob(
        stage: String,
        url: String,
        advertisedSize: Option[Long]
    )

    /** Resolve the HTTP URL for a single immutable-chunk archive by substituting
      * `{immutable_file_number}` in the first `CloudStorage` template in `meta.immutables`.
      */
    def immutableUrl(meta: MithrilMessages.CardanoDatabaseV2Metadata, n: Long): String =
        meta.immutables.locations
            .collectFirst {
                case MithrilMessages.ImmutablesLocation.CloudStorage(tmpl, _)
                    if tmpl.template.isDefined =>
                    tmpl.resolve(n).get
            }
            .getOrElse(
              throw new IllegalStateException(
                "no CloudStorage Template URI in immutables.locations — " +
                    "aggregator returned unexpected shape"
              )
            )

    /** Construct a client around a fresh WASM instance wired to `aggregatorUrl`. Caller owns the
      * returned client and must [[MithrilClient.close]] it when done.
      *
      * `onFetch` observes every WASM-initiated HTTP request — issued when the request is dispatched
      * and again when the response settles. The chain-walk inside [[verifyCertificateChain]] is
      * otherwise opaque (one fetch per cert, hundreds of hops on preview); install a listener to
      * surface progress.
      */
    def create(
        aggregatorUrl: String,
        genesisVerificationKey: String,
        hashes: MithrilAsyncRuntime.ClosureHashes = MithrilAsyncRuntime.ClosureHashes.Release0_10_4,
        onFetch: MithrilAsyncRuntime.FetchEvent => Unit = _ => (),
        executionListener: Option[com.dylibso.chicory.runtime.ExecutionListener] = None
    )(using ec: ExecutionContext): MithrilClient = {
        val abi = new WbindgenAbi(hashes)
        val asyncRt = new MithrilAsyncRuntime(abi, hashes, onFetch)
        val imports = abi.defaultImports ++ abi.pinnedImports ++ asyncRt.asyncImports
        val (rt, _) = MithrilWasmRuntime.instantiate(imports, executionListener)
        asyncRt.attach(rt.instance)

        val (aggPtr, aggLen) = rt.passString(aggregatorUrl)
        val (keyPtr, keyLen) = rt.passString(genesisVerificationKey)
        val clientPtr = rt
            .exportFn("mithrilclient_new")
            .apply(
              aggPtr.toLong,
              aggLen.toLong,
              keyPtr.toLong,
              keyLen.toLong,
              1L
            )(0)

        new MithrilClient(rt, asyncRt, clientPtr, HttpClient.newHttpClient())
    }

    /** Build a client whose only live dependency is the JVM `HttpClient` — no WASM, no
      * async-runtime dispatcher. Intended for unit-testing the HTTP + tar+zstd path without
      * standing up the full cryptographic client. `close()` on the returned instance is a no-op
      * because there's nothing to release (calling the WASM/dispatcher-bound methods will NPE).
      */
    private[mithril] def forHttpOnly(httpClient: HttpClient)(using
        ExecutionContext
    ): MithrilClient =
        new MithrilClient(null, null, 0L, httpClient)
}
