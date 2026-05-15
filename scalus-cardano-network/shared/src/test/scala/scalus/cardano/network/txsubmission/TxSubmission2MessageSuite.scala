package scalus.cardano.network.txsubmission

import io.bullet.borer.Cbor
import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.network.txsubmission.TxSubmission2Message.*
import scalus.uplc.builtin.ByteString
import scalus.utils.Hex.*

class TxSubmission2MessageSuite extends AnyFunSuite {

    private def roundTrip(m: TxSubmission2Message): Unit = {
        val bytes = Cbor.encode(m).toByteArray
        val decoded = Cbor.decode(bytes).to[TxSubmission2Message].value
        assert(decoded == m, s"round-trip mismatch for $m\nbytes=${bytes.toHex}")
    }

    private val hashA: ByteString =
        ByteString.fromArray(Array.tabulate(32)(i => i.toByte))
    private val hashB: ByteString =
        ByteString.fromArray(Array.tabulate(32)(i => (0xff - i).toByte))

    test("round-trip MsgInit") {
        roundTrip(MsgInit)
    }

    test("round-trip MsgRequestTxIds — blocking, non-zero ack/req") {
        roundTrip(MsgRequestTxIds(blocking = true, numAck = 3, numReq = 5))
        roundTrip(MsgRequestTxIds(blocking = false, numAck = 0, numReq = 0xffff))
    }

    test("round-trip MsgReplyTxIds — empty + multi-entry") {
        roundTrip(MsgReplyTxIds(Seq.empty))
        roundTrip(
          MsgReplyTxIds(
            Seq(
              TxId(era = 6, hashA) -> 1024L,
              TxId(era = 6, hashB) -> 4_000_000_000L
            )
          )
        )
    }

    test("round-trip MsgRequestTxs — single + multi-id") {
        roundTrip(MsgRequestTxs(Seq(TxId(era = 6, hashA))))
        roundTrip(MsgRequestTxs(Seq(TxId(era = 6, hashA), TxId(era = 6, hashB))))
    }

    test("round-trip MsgReplyTxs — wraps each body in tag 24") {
        val body1 = ByteString.fromArray(Array[Byte](0xa0.toByte))
        val body2 = ByteString.fromArray(Array[Byte](0x82.toByte, 0x01, 0x02))
        roundTrip(MsgReplyTxs(Seq(TxBody(era = 6, body1), TxBody(era = 6, body2))))
    }

    test("round-trip MsgDone") {
        roundTrip(MsgDone)
    }

    test("golden: MsgInit is [6]") {
        // 81  array(1)
        //   06  uint 6
        assert(Cbor.encode(MsgInit: TxSubmission2Message).toByteArray.toHex == "8106")
    }

    test("golden: MsgDone is [4]") {
        assert(Cbor.encode(MsgDone: TxSubmission2Message).toByteArray.toHex == "8104")
    }

    test("golden: MsgRequestTxIds(false, 0, 1) is [0, false, 0, 1]") {
        // 84    array(4)
        //   00  uint 0   ; tag
        //   f4  bool false (simple value 20)
        //   00  uint 0   ; numAck
        //   01  uint 1   ; numReq
        val m: TxSubmission2Message = MsgRequestTxIds(blocking = false, numAck = 0, numReq = 1)
        assert(Cbor.encode(m).toByteArray.toHex == "8400f40001")
    }

    test("validation: numAck / numReq must be in u16 range") {
        intercept[IllegalArgumentException] {
            MsgRequestTxIds(blocking = true, numAck = -1, numReq = 0)
        }
        intercept[IllegalArgumentException] {
            MsgRequestTxIds(blocking = true, numAck = 0, numReq = 0x10000)
        }
    }
}
