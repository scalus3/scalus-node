package scalus.cardano.network.n2c.localstatequery

import io.bullet.borer.Cbor
import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.ledger.BlockHash
import scalus.cardano.network.chainsync.Point
import scalus.serialization.cbor.Cbor as ScalusCbor
import scalus.uplc.builtin.ByteString
import scalus.utils.Hex.*

class LsqQuerySuite extends AnyFunSuite {

    private def encHex(q: LsqQuery[?]): String = ScalusCbor.encode(q).toHex

    test("GetChainPoint encodes as [3]") {
        assert(encHex(LsqQuery.GetChainPoint) == "8103")
    }

    test("GetChainPoint result decodes Origin") {
        val resultBytes = Array[Byte](0x80.toByte)
        val decoded = LsqQuery.GetChainPoint.decode(resultBytes)
        assert(decoded == Point.Origin)
    }

    test("GetChainPoint result decodes BlockPoint") {
        val hash = BlockHash.fromByteString(ByteString.fromArray(Array.fill[Byte](32)(0x11)))
        val point: Point = Point.BlockPoint(slot = 42L, hash = hash)
        val resultBytes = Cbor.encode(point).toByteArray
        val decoded = LsqQuery.GetChainPoint.decode(resultBytes)
        assert(decoded == point)
    }

    test("GetCurrentPParams Conway (era=6) encodes as [0, [0, [6, [3]]]]") {
        // 82  array(2)
        //   00  uint 0                     ; top: BlockQuery
        //   82  array(2)
        //     00  uint 0                   ; BlockQuery: QueryIfCurrent
        //     82  array(2)
        //       06  uint 6                 ; era=Conway
        //       81  array(1)
        //         03  uint 3               ; shelleyQuery: GetCurrentPParams
        val q = LsqQuery.GetCurrentPParams(era = 6, decoder = _ => ())
        assert(encHex(q) == "8200820082068103")
    }

    test("GetCurrentPParams Shelley (era=1) encodes with [1, [3]]") {
        val q = LsqQuery.GetCurrentPParams(era = 1, decoder = _ => ())
        assert(encHex(q) == "8200820082018103")
    }

    test("QueryIfCurrent envelope: [0, inner] passes inner bytes to decodeInner") {
        // Server-side response: success in current era. Envelope is [0, <inner>] = 82 00 <inner>.
        // Inner here is the byte `0xAA`, which the injected decoder echoes back to its caller.
        val inner = Array[Byte](0xaa.toByte)
        val envelope: Array[Byte] = Array(0x82.toByte, 0x00.toByte) ++ inner
        val q = LsqQuery.GetCurrentPParams[Array[Byte]](era = 6, decoder = identity)
        val decoded = q.decode(envelope)
        assert(decoded.sameElements(inner))
    }

    test("QueryIfCurrent envelope: [1, mismatch] raises LsqEraMismatchException") {
        val mismatchPayload = Array[Byte](0x01, 0x02, 0x03)
        val envelope: Array[Byte] = Array(0x82.toByte, 0x01.toByte) ++ mismatchPayload
        val q = LsqQuery.GetCurrentPParams[Array[Byte]](era = 7, decoder = identity)
        val ex = intercept[LsqEraMismatchException](q.decode(envelope))
        assert(ex.queriedEra == 7)
        assert(ex.mismatchCbor.sameElements(mismatchPayload))
    }

    test("QueryIfCurrent envelope rejects bad outer header") {
        val q = LsqQuery.GetCurrentPParams[Unit](era = 6, decoder = _ => ())
        val bad = Array[Byte](0x83.toByte, 0x00, 0x00) // array(3), not array(2)
        intercept[IllegalArgumentException](q.decode(bad))
    }

    test("QueryIfCurrent envelope rejects unknown tag") {
        val q = LsqQuery.GetCurrentPParams[Unit](era = 6, decoder = _ => ())
        val bad = Array[Byte](0x82.toByte, 0x02, 0x00) // tag=2, neither result nor mismatch
        intercept[IllegalArgumentException](q.decode(bad))
    }

    test("GetCurrentPParams.decode delegates to injected decoder after envelope peel") {
        val sentinel = new Object
        val q = LsqQuery.GetCurrentPParams[AnyRef](era = 6, decoder = _ => sentinel)
        val envelope: Array[Byte] = Array(0x82.toByte, 0x00.toByte, 0x01.toByte)
        assert(q.decode(envelope) eq sentinel)
    }

    test("GetUTxOByAddress (era=6, empty set) encodes envelope + [6, tag258 array(0)]") {
        // Envelope (8 bytes):       82 00 82 00 82 06 82 06
        // ShelleyQuery body:        82 06 - already counted above as the last two
        // (re-grouped) — actual layout per bytes:
        //   82 00              top: [0, BlockQuery]
        //   82 00              BlockQuery: [0, QueryIfCurrent]
        //   82 06              [era=Conway, ...]
        //   82 06              shelleyQuery: [6, addrs]
        //   D9 01 02           tag 258 (CBOR set marker)
        //   80                 array(0) — empty set
        val q = LsqQuery.GetUTxOByAddress(era = 6, addresses = Set.empty)
        assert(encHex(q) == "8200820082068206d9010280")
    }

    test("GetUTxOByTxIn (era=6, empty set) encodes envelope + [15, tag258 array(0)]") {
        //   82 06              shelleyQuery: [15=0F, inputs]
        val q = LsqQuery.GetUTxOByTxIn(era = 6, inputs = Set.empty)
        assert(encHex(q) == "820082008206820fd9010280")
    }

    test("GetUTxOByAddress.decode parses empty Utxos map") {
        // Envelope: [0, a0]  — success branch + CBOR map(0)
        val envelope: Array[Byte] = Array(0x82.toByte, 0x00.toByte, 0xa0.toByte)
        val q = LsqQuery.GetUTxOByAddress(era = 6, addresses = Set.empty)
        val utxos = q.decode(envelope)
        assert(utxos.isEmpty)
    }

    test("GetUTxOByTxIn.decode parses empty Utxos map") {
        val envelope: Array[Byte] = Array(0x82.toByte, 0x00.toByte, 0xa0.toByte)
        val q = LsqQuery.GetUTxOByTxIn(era = 6, inputs = Set.empty)
        val utxos = q.decode(envelope)
        assert(utxos.isEmpty)
    }

    test("GetUTxOByAddress.decode surfaces EraMismatch") {
        val envelope: Array[Byte] = Array(0x82.toByte, 0x01.toByte, 0xff.toByte)
        val q = LsqQuery.GetUTxOByAddress(era = 6, addresses = Set.empty)
        val ex = intercept[LsqEraMismatchException](q.decode(envelope))
        assert(ex.queriedEra == 6)
    }
}
