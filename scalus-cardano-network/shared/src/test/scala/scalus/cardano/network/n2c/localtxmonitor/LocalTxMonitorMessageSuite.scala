package scalus.cardano.network.n2c.localtxmonitor

import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.ledger.TransactionHash
import scalus.cardano.network.n2c.localtxmonitor.LocalTxMonitorMessage.*
import scalus.serialization.cbor.Cbor
import scalus.uplc.builtin.ByteString
import scalus.utils.Hex.*

class LocalTxMonitorMessageSuite extends AnyFunSuite {

    private def roundTrip(m: LocalTxMonitorMessage): Unit = {
        val bytes = Cbor.encode(m)
        val decoded = io.bullet.borer.Cbor.decode(bytes).to[LocalTxMonitorMessage].value
        assert(decoded == m, s"round-trip mismatch for $m\nbytes=${bytes.toHex}")
    }

    private def encHex(m: LocalTxMonitorMessage): String = Cbor.encode(m).toHex

    private val sampleTxHash: TransactionHash =
        TransactionHash.fromByteString(ByteString.fromArray(Array.fill[Byte](32)(0xab.toByte)))

    // ---------------- round-trip ----------------

    test("round-trip MsgDone") { roundTrip(MsgDone) }
    test("round-trip MsgAcquire") { roundTrip(MsgAcquire) }
    test("round-trip MsgAcquired(slot)") { roundTrip(MsgAcquired(slot = 12345L)) }
    test("round-trip MsgRelease") { roundTrip(MsgRelease) }
    test("round-trip MsgHasTx") { roundTrip(MsgHasTx(sampleTxHash)) }
    test("round-trip MsgRespondHasTx(true)") { roundTrip(MsgRespondHasTx(true)) }
    test("round-trip MsgRespondHasTx(false)") { roundTrip(MsgRespondHasTx(false)) }

    // ---------------- golden bytes ----------------

    test("golden: single-tag messages") {
        // 81 NN  = array(1) [N]
        assert(encHex(MsgDone) == "8100")
        assert(encHex(MsgAcquire) == "8101")
        assert(encHex(MsgRelease) == "8103")
    }

    test("golden: MsgAcquired(slot=10) — [2, 10]") {
        // 82  array(2)
        //   02  uint 2
        //   0a  uint 10
        assert(encHex(MsgAcquired(10L)) == "82020a")
    }

    test("golden: MsgHasTx — [7, bytes(32)]") {
        // 82  array(2)
        //   07  uint 7
        //   58 20 <32 bytes>  ─ byte string len 32
        val hex = encHex(MsgHasTx(sampleTxHash))
        assert(hex.startsWith("82075820"))
        assert(hex == "8207582" + "0" + "ab" * 32)
    }

    test("golden: MsgRespondHasTx — [8, bool]") {
        // 82  array(2)
        //   08  uint 8
        //   f4 / f5  ─ CBOR false / true
        assert(encHex(MsgRespondHasTx(false)) == "8208f4")
        assert(encHex(MsgRespondHasTx(true)) == "8208f5")
    }

    // ---------------- decoding rejection ----------------

    test("decoding rejects unknown tag") {
        // [99]
        val malformed: Array[Byte] = Array(0x81.toByte, 0x18.toByte, 99.toByte)
        intercept[Throwable] {
            io.bullet.borer.Cbor.decode(malformed).to[LocalTxMonitorMessage].value
        }
    }

    test("decoding rejects wrong arrLen for MsgAcquired") {
        // [2]  with arrLen=1, missing the slot — should fail
        val malformed: Array[Byte] = Array(0x81.toByte, 0x02.toByte)
        intercept[Throwable] {
            io.bullet.borer.Cbor.decode(malformed).to[LocalTxMonitorMessage].value
        }
    }
}
