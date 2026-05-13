package scalus.cardano.network.it

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import scalus.cardano.address.Address
import scalus.cardano.ledger.{CardanoInfo, TransactionHash, TransactionInput}
import scalus.cardano.network.NetworkMagic
import scalus.cardano.network.n2c.LocalNodeAccess
import scalus.cardano.node.{UtxoQuery, UtxoQueryError, UtxoSource}
import scalus.uplc.builtin.ByteString

import java.nio.file.{Files, Paths}
import scala.concurrent.ExecutionContext.Implicits.global

/** End-to-end IT for [[LocalNodeAccess]] against a real Cardano-node N2C socket. Yaci-devkit's
  * `YaciCardanoContainer` doesn't expose the node Unix socket through its Java API (it surfaces only
  * the N2N TCP port, Ogmios, and a couple of HTTP services), so this suite is env-gated — set
  * `SCALUS_N2C_SOCKET` to a Unix-socket path the test process can `connect()` on, optionally
  * `SCALUS_N2C_NETWORK_MAGIC` (default 42 = yaci-devnet) and `SCALUS_N2C_ERA` (default 6 = Conway).
  *
  * The `fetchLatestParams` test is Conway-only — `LocalNodeAccess` hardcodes the Conway PParams
  * decoder — and skips when `SCALUS_N2C_ERA` is not 6 (e.g. yaci-devkit defaults to Babbage; that
  * test will then skip while the rest of the suite still runs).
  *
  * Recommended local setup:
  * {{{
  *   docker run --rm -v $PWD/yaci-sock:/node/db -p 3001:3001 bloxbean/yaci-cli
  *   # then: export SCALUS_N2C_SOCKET=$PWD/yaci-sock/node.socket
  * }}}
  *
  * The probe exercises the LSQ paths that the unit-test suite can only verify against hand-rolled
  * CBOR golden bytes — the same approach that hid the M12.P2 envelope bug until a real node spoke
  * back.
  */
class LocalNodeAccessN2cProbe extends AnyFunSuite with ScalaFutures {

    implicit override val patienceConfig: PatienceConfig =
        PatienceConfig(timeout = Span(60, Seconds), interval = Span(200, Millis))

    private def socketPath: java.nio.file.Path = sys.env.get("SCALUS_N2C_SOCKET") match {
        case Some(p) =>
            val path = Paths.get(p)
            assume(Files.exists(path), s"SCALUS_N2C_SOCKET=$p does not exist")
            path
        case None =>
            assume(false, "SCALUS_N2C_SOCKET not set")
            throw new IllegalStateException("unreachable") // for the type-checker
    }

    private def networkMagic: NetworkMagic =
        sys.env
            .get("SCALUS_N2C_NETWORK_MAGIC")
            .map(_.toLong)
            .map(NetworkMagic.apply)
            .getOrElse(NetworkMagic.YaciDevnet)

    private def submitEra: Int =
        sys.env.get("SCALUS_N2C_ERA").map(_.toInt).getOrElse(6)

    /** A bech32 testnet address used to exercise `findUtxos` against a live node. `lazy val`
      * because parsing happens only inside tests, after `assume(SCALUS_N2C_SOCKET)` has gated
      * suite construction.
      */
    private lazy val testnetAddress: Address = Address.fromBech32(
      "addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp"
    )

    private def withProvider(test: LocalNodeAccess => Unit): Unit = {
        val provider = LocalNodeAccess
            .connect(socketPath, networkMagic, CardanoInfo.preview, submitEra)
            .futureValue
        try test(provider)
        finally provider.close().futureValue
    }

    test("currentSlot returns a positive slot number from the live node") {
        withProvider { p =>
            val slot = p.currentSlot.futureValue
            assert(slot > 0L, s"expected slot > 0, got $slot")
        }
    }

    test("fetchLatestParams returns sane Conway ProtocolParams") {
        assume(submitEra == 6, s"SCALUS_N2C_ERA=$submitEra; this test is Conway-only")
        withProvider { p =>
            val pp = p.fetchLatestParams.futureValue
            // Sanity-check fields that must be non-zero in any reasonable Conway snapshot.
            assert(pp.txFeePerByte > 0L, s"txFeePerByte=${pp.txFeePerByte}")
            assert(pp.txFeeFixed > 0L, s"txFeeFixed=${pp.txFeeFixed}")
            assert(pp.maxTxSize > 0L, s"maxTxSize=${pp.maxTxSize}")
            assert(pp.maxBlockBodySize > 0L, s"maxBlockBodySize=${pp.maxBlockBodySize}")
            assert(pp.stakePoolDeposit > 0L, s"stakePoolDeposit=${pp.stakePoolDeposit}")
            assert(pp.utxoCostPerByte > 0L, s"utxoCostPerByte=${pp.utxoCostPerByte}")
            assert(pp.protocolVersion.major >= 9, s"protocolVersion=${pp.protocolVersion}")
        }
    }

    test("findUtxos by-address returns a (possibly empty) map for an unused address") {
        withProvider { p =>
            val q = UtxoQuery(UtxoSource.FromAddress(testnetAddress))
            val result = p.findUtxos(q).futureValue
            assert(result.isRight, s"expected Right, got $result")
            // No assertion on the size — production state moves; we only assert no fault.
        }
    }

    test("findUtxos by-inputs returns a (possibly empty) map for an unused input") {
        withProvider { p =>
            val nonExistent = TransactionInput(
              transactionId = TransactionHash.fromArray(Array.fill[Byte](32)(0x00)),
              index = 0
            )
            val q = UtxoQuery(UtxoSource.FromInputs(Set(nonExistent)))
            val result = p.findUtxos(q).futureValue
            assert(result.isRight, s"expected Right, got $result")
            assert(result.exists(_.isEmpty), "all-zero TxIn should resolve to no UTxO")
        }
    }

    test("findUtxos with an unsupported query shape returns NotSupported") {
        withProvider { p =>
            val q = UtxoQuery(UtxoSource.FromAddress(testnetAddress)).limit(10)
            val result = p.findUtxos(q).futureValue
            result match {
                case Left(_: UtxoQueryError.NotSupported) => () // ok
                case other =>
                    fail(s"expected NotSupported for query with limit, got $other")
            }
        }
    }

    test("getDatum returns None — LSQ has no datum-by-hash query") {
        withProvider { p =>
            val datumHash = scalus.cardano.ledger.DataHash.fromByteString(
              ByteString.fromArray(Array.fill[Byte](32)(0x55))
            )
            val result = p.getDatum(datumHash).futureValue
            assert(result.isEmpty)
        }
    }

    test("checkInMempool for an unknown hash returns false via LocalTxMonitor") {
        withProvider { p =>
            val unknown = TransactionHash.fromByteString(
              ByteString.fromArray(Array.fill[Byte](32)(0x55))
            )
            assert(!p.checkInMempool(unknown).futureValue)
        }
    }
}
