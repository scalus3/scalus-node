package scalus.cardano.network.n2c.localtxsubmission

import io.bullet.borer.Cbor
import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.network.n2c.localtxsubmission.LocalTxSubmissionMessage.*
import scalus.uplc.builtin.ByteString
import scalus.utils.Hex.*

class LocalTxSubmissionMessageSuite extends AnyFunSuite {

    private def roundTrip(m: LocalTxSubmissionMessage): Unit = {
        val bytes = Cbor.encode(m).toByteArray
        val decoded = Cbor.decode(bytes).to[LocalTxSubmissionMessage].value
        assert(decoded == m, s"round-trip mismatch for $m\nbytes=${bytes.toHex}")
    }

    test("round-trip MsgSubmitTx (Conway era, short tx)") {
        roundTrip(MsgSubmitTx(era = 6, ByteString.fromArray(Array[Byte](0xa0.toByte))))
    }

    test("round-trip MsgAcceptTx") {
        roundTrip(MsgAcceptTx)
    }

    test("round-trip MsgRejectTx (era + reason CBOR)") {
        roundTrip(
          MsgRejectTx(era = 6, ByteString.fromArray(Array[Byte](0x82.toByte, 0x00, 0x01)))
        )
    }

    test("round-trip MsgDone") {
        roundTrip(MsgDone)
    }

    test("golden: MsgAcceptTx is a single-element array [1]") {
        val m: LocalTxSubmissionMessage = MsgAcceptTx
        // 81  array(1)
        //   01  uint 1     ; tag
        assert(Cbor.encode(m).toByteArray.toHex == "8101")
    }

    test("golden: MsgDone is a single-element array [3]") {
        val m: LocalTxSubmissionMessage = MsgDone
        assert(Cbor.encode(m).toByteArray.toHex == "8103")
    }

    test("golden: MsgSubmitTx HFC envelope shape — [0, [era, tag24(bytes)]]") {
        // 82            array(2)         ; outer
        //   00          uint 0           ; tag = MsgSubmitTx
        //   82          array(2)         ; HFC envelope
        //     06        uint 6           ; era = Conway
        //     d818      tag(24)
        //     41 a0     bytes(1) 0xa0    ; one-byte body (empty CBOR map)
        val m: LocalTxSubmissionMessage =
            MsgSubmitTx(era = 6, ByteString.fromArray(Array[Byte](0xa0.toByte)))
        assert(Cbor.encode(m).toByteArray.toHex == "82008206d81841a0")
    }

    test("decoding rejects a too-long array for MsgAcceptTx") {
        // [1, 0] — accept-tx with extra payload
        val malformed = Array[Byte](0x82.toByte, 0x01, 0x00)
        intercept[Throwable] {
            Cbor.decode(malformed).to[LocalTxSubmissionMessage].value
        }
    }
}
