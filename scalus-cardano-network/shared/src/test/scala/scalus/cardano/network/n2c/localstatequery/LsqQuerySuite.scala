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
        val decoded = LsqQuery.decodeResult(LsqQuery.GetChainPoint, resultBytes)
        assert(decoded == Point.Origin)
    }

    test("GetChainPoint result decodes BlockPoint") {
        // [42, hash32]
        val hash = BlockHash.fromByteString(ByteString.fromArray(Array.fill[Byte](32)(0x11)))
        val point: Point = Point.BlockPoint(slot = 42L, hash = hash)
        val resultBytes = Cbor.encode(point).toByteArray
        val decoded = LsqQuery.decodeResult(LsqQuery.GetChainPoint, resultBytes)
        assert(decoded == point)
    }
}
