package scalus.cardano.node.stream.engine.snapshot

import scalus.cardano.ledger.{TransactionInput, TransactionOutput}
import scalus.cardano.node.stream.{ChainTip, SnapshotSource, UnsupportedSourceException}
import scalus.cardano.node.stream.engine.{ChainStore, ChainStoreUtxoSet}
import scalus.uplc.builtin.ByteString

import java.io.{BufferedInputStream, InputStream}
import java.nio.file.Files
import java.security.{DigestInputStream, MessageDigest}
import scala.concurrent.{ExecutionContext, Future}

import ChainStoreSnapshot.Record

/** Drives a cold-start bootstrap: resolve a [[SnapshotSource]] to a populated [[ChainStore]]
  * (and its optional [[ChainStoreUtxoSet]]).
  *
  * Two restore shapes coexist:
  *
  *   - **Streaming-CBOR** ([[SnapshotSource.File]] / [[SnapshotSource.Url]]) drives
  *     [[SnapshotReader]] over the Scalus-native interchange format defined in M10.P1. One
  *     record at a time, never materialised in full.
  *   - **Mithril dir-restore** ([[SnapshotSource.Mithril]] / [[SnapshotSource.MithrilDir]])
  *     delegates to a [[MithrilSnapshotResolver]] discovered via `ServiceLoader` from
  *     `scalus-chain-store-mithril`. The resolver knows how to download + verify a cardano-node
  *     V2 artefact, unpack ImmutableDB chunks + the ledger-state, and populate the store via
  *     the same `appendBlock` / `restoreUtxoSet` calls.
  *
  * Used by the Fs2 / Ox providers at construction time when `config.bootstrap` is set and the
  * engine has no warm-restart tip. See `docs/local/claude/indexer/snapshot-bootstrap-m10.md` for
  * the cold-vs-warm startup rules.
  */
final class ChainStoreRestorer(store: ChainStore) {

    private val utxoSet: Option[ChainStoreUtxoSet] = store match {
        case u: ChainStoreUtxoSet => Some(u)
        case _                    => None
    }

    /** Restore `source` into `store`. Returns the snapshot's tip on success; fails the Future with
      * [[SnapshotError]] on corruption / unsupported sources / I/O failure.
      */
    def restore(source: SnapshotSource)(using ExecutionContext): Future[ChainTip] = source match {
        case f: SnapshotSource.File       => streamRestore(f.path, expectedSha256 = None)
        case u: SnapshotSource.Url        => urlRestore(u)
        case m: SnapshotSource.Mithril    => mithrilRestore(m)
        case d: SnapshotSource.MithrilDir => mithrilDirRestore(d)
    }

    // ------------------------------------------------------------------
    // Streaming CBOR (File / Url) — the M10.P1 path.
    // ------------------------------------------------------------------

    private def streamRestore(
        path: java.nio.file.Path,
        expectedSha256: Option[ByteString]
    )(using ExecutionContext): Future[ChainTip] =
        Future(Files.newInputStream(path)).map(in => runStream(in, expectedSha256))

    private def urlRestore(u: SnapshotSource.Url)(using ExecutionContext): Future[ChainTip] =
        Future {
            val conn = new java.net.URI(u.url).toURL.openConnection()
            conn.setConnectTimeout(30_000)
            conn.setReadTimeout(60_000)
            conn.getInputStream
        }.map(in => runStream(in, u.expectedSha256))

    private def runStream(in: InputStream, expectedSha256: Option[ByteString]): ChainTip = {
        // Wrap with DigestInputStream so Url's expectedSha256 can be cross-checked after the
        // reader has drained the body.
        val digest = MessageDigest.getInstance("SHA-256")
        val wrapped = new DigestInputStream(new BufferedInputStream(in), digest)
        try {
            val tip = runRestore(wrapped)
            expectedSha256.foreach { expected =>
                val got = digest.digest()
                if !java.util.Arrays.equals(got, expected.bytes) then
                    throw SnapshotError.SnapshotCorrupted(
                      "url sha256 mismatch: downloaded snapshot does not match expectedSha256"
                    )
            }
            tip
        } finally wrapped.close()
    }

    // ------------------------------------------------------------------
    // Mithril dir-restore — SPI dispatch into scalus-chain-store-mithril.
    // ------------------------------------------------------------------

    private def mithrilRestore(
        m: SnapshotSource.Mithril
    )(using ExecutionContext): Future[ChainTip] = withResolver { (resolver, store) =>
        resolver.restore(m, store)
    }

    private def mithrilDirRestore(
        d: SnapshotSource.MithrilDir
    )(using ExecutionContext): Future[ChainTip] = withResolver { (resolver, store) =>
        resolver.restoreDir(d, store)
    }

    private def withResolver(
        body: (MithrilSnapshotResolver, ChainStore & ChainStoreUtxoSet) => Future[ChainTip]
    )(using ExecutionContext): Future[ChainTip] =
        utxoSet match {
            case None =>
                Future.failed(
                  SnapshotError.SnapshotConfigError(
                    "Mithril snapshot restore requires a ChainStore that implements " +
                        "ChainStoreUtxoSet — the Mithril V2 artefact contains a ledger state " +
                        "the resolver bulk-loads via restoreUtxoSet"
                  )
                )
            case Some(u) =>
                MithrilSnapshotResolver.find() match {
                    case None =>
                        Future.failed(
                          UnsupportedSourceException(
                            "no MithrilSnapshotResolver registered — add the " +
                                "scalus-chain-store-mithril module to the classpath. " +
                                "See docs/local/claude/indexer/indexer-node.md M10b."
                          )
                        )
                    case Some(r) => body(r, store.asInstanceOf[ChainStore & ChainStoreUtxoSet])
                }
        }

    /** Drive the reader: stream blocks into `appendBlock`, stream UTxO entries into
      * `restoreUtxoSet` via an iterator adapter so neither the reader's blocks nor its UTxOs ever
      * accumulate in JVM heap beyond the current record.
      */
    private def runRestore(in: InputStream): ChainTip = {
        val reader = SnapshotReader(in)
        val header = reader.readHeader()
        val tip = header.tip
        val recordIt = reader.records().buffered

        // Phase 1: consume every block record at the front of the record stream. The writer is
        // conventionally "blocks then UTxOs", but the reader tolerates interleaved by iterating
        // greedily while the head is a block.
        var blocksSeen = 0L
        while recordIt.hasNext && recordIt.head.isInstanceOf[Record.BlockRecord] do {
            recordIt.next() match {
                case Record.BlockRecord(b) =>
                    store.appendBlock(b)
                    blocksSeen += 1
                case _ => // impossible; checked via isInstanceOf
            }
        }

        // Phase 2: the remainder is UTxO entries. Stream them into the store in one call so the
        // backend can pipeline writes in batches without an intermediate full-heap materialisation.
        // The counter is a var captured by the anonymous iterator — kept here rather than on a
        // local class so the `utxoIt` expression inlines.
        var utxosSeen: Long = 0L
        val utxoIt: Iterator[(TransactionInput, TransactionOutput)] =
            new Iterator[(TransactionInput, TransactionOutput)] {
                def hasNext: Boolean = recordIt.hasNext
                def next(): (TransactionInput, TransactionOutput) = recordIt.next() match {
                    case Record.UtxoEntry(i, o) =>
                        utxosSeen += 1
                        (i, o)
                    case other =>
                        throw SnapshotError.SnapshotCorrupted(
                          s"unexpected record type in UTxO body region: $other"
                        )
                }
            }

        if header.hasUtxoSet then {
            utxoSet match {
                case Some(u) =>
                    u.restoreUtxoSet(tip, utxoIt)
                case None =>
                    throw SnapshotError.SnapshotConfigError(
                      "snapshot advertises a UTxO set (contentFlags) but the configured " +
                          "ChainStore does not implement ChainStoreUtxoSet"
                    )
            }
        } else {
            // Header doesn't promise a UTxO set; drain any entries defensively so the reader can
            // read and validate the footer. A non-UTxO-advertising snapshot with stray entries
            // is malformed — fail before `finish()`.
            while utxoIt.hasNext do
                throw SnapshotError.SnapshotCorrupted(
                  "snapshot header has UtxoSet flag clear but body contains UtxoEntry records"
                )
        }

        reader.finish()

        // Cross-check the header's advisory counts against observations.
        if header.blockCount != blocksSeen then
            throw SnapshotError.SnapshotCorrupted(
              s"header blockCount=${header.blockCount} but observed $blocksSeen blocks"
            )
        if header.utxoCount != utxosSeen then
            throw SnapshotError.SnapshotCorrupted(
              s"header utxoCount=${header.utxoCount} but observed $utxosSeen utxos"
            )

        tip
    }
}

object ChainStoreRestorer {

    def apply(store: ChainStore): ChainStoreRestorer = new ChainStoreRestorer(store)
}
