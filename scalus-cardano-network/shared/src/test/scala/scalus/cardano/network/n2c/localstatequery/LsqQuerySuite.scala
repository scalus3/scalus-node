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
        // 81  array(1)
        //   03  uint 3
        assert(encHex(LsqQuery.GetChainPoint) == "8103")
    }

    test("GetChainPoint result decodes Origin") {
        // []  -> Point.Origin
        val resultBytes = Array[Byte](0x80.toByte)
        val decoded = LsqQuery.GetChainPoint.decode(resultBytes)
        assert(decoded == Point.Origin)
    }

    test("GetChainPoint result decodes BlockPoint") {
        // [42, hash32]
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

    test("GetCurrentPParams.decode delegates to injected decoder") {
        val sentinel = new Object
        val q = LsqQuery.GetCurrentPParams[AnyRef](era = 6, decoder = _ => sentinel)
        assert(q.decode(Array[Byte](0x01)) eq sentinel)
    }
}
