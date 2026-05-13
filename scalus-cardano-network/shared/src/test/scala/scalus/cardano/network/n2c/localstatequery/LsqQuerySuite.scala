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
        assert(decoded == Right(Point.Origin))
    }

    test("GetChainPoint result decodes BlockPoint") {
        val hash = BlockHash.fromByteString(ByteString.fromArray(Array.fill[Byte](32)(0x11)))
        val point: Point = Point.BlockPoint(slot = 42L, hash = hash)
        val resultBytes = Cbor.encode(point).toByteArray
        val decoded = LsqQuery.GetChainPoint.decode(resultBytes)
        assert(decoded == Right(point))
    }

    test("GetCurrentEra encodes as [0, [2, [1]]] = 82 00 82 02 81 01") {
        // 82  array(2)
        //   00  uint 0                 ; top: BlockQuery
        //   82  array(2)
        //     02  uint 2               ; BlockQuery: QueryHardFork
        //     81  array(1)
        //       01  uint 1             ; QueryHardFork: GetCurrentEra
        assert(encHex(LsqQuery.GetCurrentEra) == "820082028101")
    }

    test("GetCurrentEra result decodes the era index") {
        // 0x06 = Conway era index in canonical single-byte CBOR.
        assert(LsqQuery.GetCurrentEra.decode(Array[Byte](0x06.toByte)) == Right(6))
        assert(LsqQuery.GetCurrentEra.decode(Array[Byte](0x05.toByte)) == Right(5))
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

    test("QueryIfCurrent envelope: [inner] passes inner bytes to decodeInner") {
        // Server-side response shape (V16+): the result is wrapped in a single-element CBOR
        // array `[<inner>]` = 81 <inner>. Inner here is the byte 0xAA, echoed via identity.
        val inner = Array[Byte](0xaa.toByte)
        val envelope: Array[Byte] = Array(0x81.toByte) ++ inner
        val q = LsqQuery.GetCurrentPParams[Array[Byte]](era = 6, decoder = identity)
        val decoded = q.decode(envelope)
        assert(decoded.exists(_.sameElements(inner)))
    }

    test("QueryIfCurrent envelope rejects unknown leading byte") {
        val q = LsqQuery.GetCurrentPParams[Unit](era = 6, decoder = _ => ())
        val bad = Array[Byte](0x83.toByte) // array(3) — neither OK(0x81) nor mismatch(0x82)
        intercept[IllegalArgumentException](q.decode(bad))
    }

    test("QueryIfCurrent envelope: array(2) parses as Left(LsqError.EraMismatch)") {
        val q = LsqQuery.GetCurrentPParams[Unit](era = 6, decoder = _ => ())
        // [[6, "Conway"], [5, "Babbage"]] — observed shape from yaci-devkit when queried at
        // an era the node isn't currently in.
        val envelope: Array[Byte] = Array(
          0x82.toByte, // array(2) outer (EraMismatch)
          0x82.toByte,
          0x06.toByte,
          0x66.toByte // EraInfo(6, "Conway")
        ) ++ "Conway".getBytes("US-ASCII") ++ Array(
          0x82.toByte,
          0x05.toByte,
          0x67.toByte // EraInfo(5, "Babbage")
        ) ++ "Babbage".getBytes("US-ASCII")
        q.decode(envelope) match {
            case Left(LsqError.EraMismatch(expected, actual)) =>
                assert(expected == EraInfo(6, "Conway"))
                assert(actual == EraInfo(5, "Babbage"))
            case other => fail(s"expected Left(EraMismatch), got $other")
        }
    }

    test("GetCurrentPParams.decode delegates to injected decoder after envelope peel") {
        val sentinel = new Object
        val q = LsqQuery.GetCurrentPParams[AnyRef](era = 6, decoder = _ => sentinel)
        val envelope: Array[Byte] = Array(0x81.toByte, 0x01.toByte)
        assert(q.decode(envelope).exists(_ eq sentinel))
    }

    test("GetUTxOByAddress (era=6, empty set) encodes envelope + [6, array(0)]") {
        //   82 00      top: [0, BlockQuery]
        //   82 00      BlockQuery: [0, QueryIfCurrent]
        //   82 06      [era=Conway, shelleyQuery]
        //   82 06      shelleyQuery: [6, addrs]
        //   80         array(0) — empty bare-array set (no tag 258; cardano-node refuses tagged
        //              sets in query parameters with DeserialiseFailure "expected list len")
        val q = LsqQuery.GetUTxOByAddress(era = 6, addresses = Set.empty)
        assert(encHex(q) == "820082008206820680")
    }

    test("GetUTxOByTxIn (era=6, empty set) encodes envelope + [15, array(0)]") {
        // Same shape as GetUTxOByAddress with shelleyQuery tag 15 (0x0F) instead of 6.
        val q = LsqQuery.GetUTxOByTxIn(era = 6, inputs = Set.empty)
        assert(encHex(q) == "820082008206820f80")
    }

    test("GetUTxOByAddress.decode parses empty Utxos map (envelope [{}] = 81 a0)") {
        val envelope: Array[Byte] = Array(0x81.toByte, 0xa0.toByte)
        val q = LsqQuery.GetUTxOByAddress(era = 6, addresses = Set.empty)
        val utxos = q.decode(envelope)
        assert(utxos.exists(_.isEmpty))
    }

    test("GetUTxOByTxIn.decode parses empty Utxos map (envelope [{}] = 81 a0)") {
        val envelope: Array[Byte] = Array(0x81.toByte, 0xa0.toByte)
        val q = LsqQuery.GetUTxOByTxIn(era = 6, inputs = Set.empty)
        val utxos = q.decode(envelope)
        assert(utxos.exists(_.isEmpty))
    }
}
