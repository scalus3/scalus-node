package scalus.cardano.node.stream.engine.snapshot.mithril

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import scalus.cardano.address.Address
import scalus.cardano.ledger.{TransactionHash, TransactionInput, TransactionOutput, Value}
import scalus.cardano.node.stream.SnapshotSource
import scalus.cardano.node.stream.engine.kvstore.InMemoryKvStore
import scalus.cardano.node.stream.engine.snapshot.{ChainStoreRestorer, MithrilSnapshotResolver}
import scalus.cardano.node.stream.engine.snapshot.immutabledb.{
    ImmutableDbReader,
    ImmutableDbRealFixtureSuite
}
import scalus.cardano.node.stream.engine.KvChainStore

import java.io.{ByteArrayOutputStream, FileOutputStream}
import java.nio.file.{Files, Path}
import scala.concurrent.ExecutionContext

/** Verifies the M10b SPI wiring: with `scalus-chain-store-mithril` on the classpath,
  * `MithrilSnapshotResolver.find()` discovers [[MithrilSnapshotResolverImpl]], and
  * [[ChainStoreRestorer]]'s [[SnapshotSource.MithrilDir]] branch dispatches through it onto
  * [[scalus.cardano.node.stream.engine.snapshot.SnapshotDirRestorer]].
  *
  * The Mithril (download) branch is exercised end-to-end by [[MithrilFullPreviewProbe]] under an
  * env-gate; this suite stays offline by using `MithrilDir`.
  */
final class MithrilSnapshotResolverSpiSuite extends AnyFunSuite with ScalaFutures {

    implicit override val patienceConfig: PatienceConfig =
        PatienceConfig(timeout = Span(60, Seconds), interval = Span(50, Millis))

    private given ExecutionContext = ExecutionContext.global

    private val ShelleyEnterpriseAddrBech =
        "addr_test1vzpwq95z3xyum8vqndgdd9mdnmafh3djcxnc6jemlgdmswcve6tkw"

    test("ServiceLoader discovers MithrilSnapshotResolverImpl") {
        val r = MithrilSnapshotResolver.find()
        assert(r.exists(_.isInstanceOf[MithrilSnapshotResolverImpl]))
    }

    test("ChainStoreRestorer drives the SPI for SnapshotSource.MithrilDir") {
        val (immutableDir, _) = ImmutableDbRealFixtureSuite.stageFixture()
        val snapshotRoot = immutableDir.getParent
        try {
            val lastSlot = new ImmutableDbReader(immutableDir).blocks().map(_.slot).max
            val ledgerSlotDir = snapshotRoot.resolve("ledger").resolve(lastSlot.toString)
            Files.createDirectories(ledgerSlotDir)
            Files.write(
              ledgerSlotDir.resolve("meta"),
              """{"backend":"utxohd-mem","checksum":0,"tablesCodecVersion":1}""".getBytes
            )
            Files.write(ledgerSlotDir.resolve("state"), Array[Byte](0x00))
            writeTablesFile(
              ledgerSlotDir.resolve("tables"),
              Seq(
                mkTxIn(0x77.toByte, 1) -> TransactionOutput
                    .Shelley(Address.fromBech32(ShelleyEnterpriseAddrBech),
                             Value.lovelace(7_777_777L), datumHash = None)
              )
            )

            val store = new KvChainStore(InMemoryKvStore())
            try {
                val tip = ChainStoreRestorer(store)
                    .restore(SnapshotSource.MithrilDir(snapshotRoot))
                    .futureValue
                assert(tip.point.slot == lastSlot)
                assert(store.tip.contains(tip))
            } finally store.close()
        } finally ImmutableDbRealFixtureSuite.cleanup(snapshotRoot)
    }

    test("MithrilDir on a non-directory path fails with SnapshotConfigError") {
        val tmp = Files.createTempFile("mithril-not-a-dir-", ".tmp")
        try {
            val store = new KvChainStore(InMemoryKvStore())
            try {
                val cause = ChainStoreRestorer(store)
                    .restore(SnapshotSource.MithrilDir(tmp))
                    .failed
                    .futureValue
                assert(
                  cause.isInstanceOf[
                    scalus.cardano.node.stream.engine.snapshot.SnapshotError.SnapshotConfigError
                  ]
                )
            } finally store.close()
        } finally Files.deleteIfExists(tmp)
    }

    test("Mithril source with invalid immutableFileRange fails with SnapshotConfigError") {
        val workDir = Files.createTempDirectory("mithril-range-test-")
        try {
            val resolver = new MithrilSnapshotResolverImpl()
            val store    = new KvChainStore(InMemoryKvStore())
            try {
                // from > to is invalid
                val causeFromGtTo = resolver
                    .restore(
                      SnapshotSource.Mithril(
                        aggregatorUrl = "https://example.com",
                        genesisVerificationKey = "key",
                        workDir = workDir,
                        immutableFileRange = Some((5L, 3L))
                      ),
                      store
                    )
                    .failed
                    .futureValue
                assert(
                  causeFromGtTo.isInstanceOf[
                    scalus.cardano.node.stream.engine.snapshot.SnapshotError.SnapshotConfigError
                  ]
                )

                // from == 0 is invalid (must be >= 1)
                val causeZeroFrom = resolver
                    .restore(
                      SnapshotSource.Mithril(
                        aggregatorUrl = "https://example.com",
                        genesisVerificationKey = "key",
                        workDir = workDir,
                        immutableFileRange = Some((0L, 5L))
                      ),
                      store
                    )
                    .failed
                    .futureValue
                assert(
                  causeZeroFrom.isInstanceOf[
                    scalus.cardano.node.stream.engine.snapshot.SnapshotError.SnapshotConfigError
                  ]
                )
            } finally store.close()
        } finally Files.deleteIfExists(workDir)
    }

    // -----------------------------------------------------------------------
    // Fixture helpers — duplicate of SnapshotDirRestorerSuite's; kept local because the two
    // suites live in different packages and these are short.

    private def mkTxIn(firstByte: Byte, index: Int): TransactionInput = {
        val hashBytes = new Array[Byte](32)
        hashBytes(0) = firstByte
        TransactionInput(TransactionHash.fromArray(hashBytes), index)
    }

    private def writeTablesFile(
        path: Path,
        entries: Seq[(TransactionInput, TransactionOutput)]
    ): Unit = {
        val buf = new ByteArrayOutputStream()
        buf.write(0x81)
        require(entries.size < 24, "fixture size must be < 24 for single-byte map header")
        buf.write(0xa0 | entries.size)
        entries.foreach { case (txIn, txOut) =>
            writeCborBytes(buf, encodeTxInMemPack(txIn))
            writeCborBytes(buf, encodeTxOutMemPack(txOut))
        }
        val os = new FileOutputStream(path.toFile)
        try os.write(buf.toByteArray)
        finally os.close()
    }

    private def encodeTxInMemPack(txIn: TransactionInput): Array[Byte] = {
        val buf = new ByteArrayOutputStream(34)
        buf.write(txIn.transactionId.bytes.toArray)
        val ix = txIn.index
        buf.write(ix & 0xff)
        buf.write((ix >>> 8) & 0xff)
        buf.toByteArray
    }

    private def encodeTxOutMemPack(txOut: TransactionOutput): Array[Byte] = {
        val buf = new ByteArrayOutputStream()
        buf.write(0x00)
        txOut match {
            case TransactionOutput.Shelley(addr, value, None) =>
                val addrBytes = addr.toBytes.bytes
                writeVarLen(buf, addrBytes.length.toLong)
                buf.write(addrBytes)
                buf.write(0x00)
                writeVarLen(buf, value.coin.value)
            case other => sys.error(s"fixture encoder only supports Shelley + ada-only, got $other")
        }
        buf.toByteArray
    }

    private def writeCborBytes(out: ByteArrayOutputStream, payload: Array[Byte]): Unit = {
        val len = payload.length
        if len < 24 then out.write(0x40 | len)
        else if len < 0x100 then { out.write(0x58); out.write(len & 0xff) }
        else if len < 0x10000 then {
            out.write(0x59); out.write((len >>> 8) & 0xff); out.write(len & 0xff)
        } else {
            out.write(0x5a)
            out.write((len >>> 24) & 0xff)
            out.write((len >>> 16) & 0xff)
            out.write((len >>> 8) & 0xff)
            out.write(len & 0xff)
        }
        out.write(payload)
    }

    private def writeVarLen(out: ByteArrayOutputStream, value: Long): Unit = {
        require(value >= 0)
        val bits = if value == 0 then 1 else 64 - java.lang.Long.numberOfLeadingZeros(value)
        val groups = (bits + 6) / 7
        var i = groups - 1
        while i >= 0 do {
            val groupVal = ((value >>> (i * 7)) & 0x7fL).toInt
            if i == 0 then out.write(groupVal) else out.write(groupVal | 0x80)
            i -= 1
        }
    }
}
