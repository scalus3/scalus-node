package scalus.cardano.network.n2c.localstatequery

import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.ledger.{BlockHash, OriginalCborByteArray}
import scalus.cardano.network.chainsync.Point
import scalus.cardano.network.n2c.localstatequery.LocalStateQueryMessage.*
import scalus.serialization.cbor.Cbor
import scalus.uplc.builtin.ByteString
import scalus.utils.Hex.*

class LocalStateQueryMessageSuite extends AnyFunSuite {

    private def roundTrip(m: LocalStateQueryMessage): Unit = {
        val bytes = Cbor.encode(m)
        given OriginalCborByteArray = OriginalCborByteArray(bytes)
        val decoded = io.bullet.borer.Cbor.decode(bytes).to[LocalStateQueryMessage].value
        assert(decoded == m, s"round-trip mismatch for $m\nbytes=${bytes.toHex}")
    }

    private def encHex(m: LocalStateQueryMessage): String = Cbor.encode(m).toHex

    // ---------------- round-trip ----------------

    test("round-trip MsgAcquire VolatileTip") {
        roundTrip(MsgAcquire(AcquireTarget.VolatileTip))
    }

    test("round-trip MsgAcquire ImmutableTip") {
        roundTrip(MsgAcquire(AcquireTarget.ImmutableTip))
    }

    test("round-trip MsgAcquire SpecificPoint(Origin)") {
        roundTrip(MsgAcquire(AcquireTarget.At(Point.Origin)))
    }

    test("round-trip MsgAcquire SpecificPoint(BlockPoint)") {
        val hash = BlockHash.fromByteString(ByteString.fromArray(Array.fill[Byte](32)(0x11)))
        roundTrip(MsgAcquire(AcquireTarget.At(Point.BlockPoint(slot = 42L, hash = hash))))
    }

    test("round-trip MsgAcquired") {
        roundTrip(MsgAcquired)
    }

    test("round-trip MsgFailure(PointTooOld)") {
        roundTrip(MsgFailure(AcquireFailure.PointTooOld))
    }

    test("round-trip MsgFailure(PointNotOnChain)") {
        roundTrip(MsgFailure(AcquireFailure.PointNotOnChain))
    }

    test("round-trip MsgQuery (raw single-element array)") {
        // body: [3]  ─ a hypothetical top-level GetChainPoint query encoding (P1.b will produce
        // these via LsqQuery encoders); shape-only test for now.
        val body = ByteString.fromArray(Array[Byte](0x81.toByte, 0x03))
        roundTrip(MsgQuery(body))
    }

    test("round-trip MsgResult (raw single-int 42)") {
        // body: 0x18 0x2a  ─ uint(42)
        val body = ByteString.fromArray(Array[Byte](0x18, 0x2a))
        roundTrip(MsgResult(body))
    }

    test("round-trip MsgResult with a tag-bearing body (captureRawCbor must not truncate)") {
        // `captureRawCbor` must not lose bytes for a body containing CBOR tags. borer's
        // `skipElement` is not tag-aware (`DataItem.Tag` ∉ `DataItem.Complex`): it skips the tag
        // header but not the tagged content, then mis-counts that content as a sibling array
        // element — silently truncating any tag-bearing body (a Conway `GetCurrentPParams` result
        // is full of tag-30 rationals). The minimal exposing shape is a tag followed by a sibling
        // element inside an array.
        // body: 82 d81e820105 03  ─ [ tag30([1, 5]), uint(3) ]
        val body = ByteString.fromArray(
          Array[Byte](
            0x82.toByte, // array(2)
            0xd8.toByte,
            0x1e,
            0x82.toByte,
            0x01,
            0x05, // tag(30) [1, 5]
            0x03 // uint(3) — the sibling the old skipElement dropped
          )
        )
        roundTrip(MsgResult(body))
    }

    test("round-trip MsgQuery with a tag-bearing body") {
        // Same tag-truncation guard on the MsgQuery path (shares captureRawCbor).
        val body = ByteString.fromArray(
          Array[Byte](0xc2.toByte, 0x42, 0x01, 0x02) // tag(2) bignum, bytes(2) 0x0102
        )
        roundTrip(MsgQuery(body))
    }

    test("round-trip MsgRelease") {
        roundTrip(MsgRelease)
    }

    test("round-trip MsgReAcquire VolatileTip") {
        roundTrip(MsgReAcquire(AcquireTarget.VolatileTip))
    }

    test("round-trip MsgReAcquire ImmutableTip") {
        roundTrip(MsgReAcquire(AcquireTarget.ImmutableTip))
    }

    test("round-trip MsgReAcquire SpecificPoint(Origin)") {
        roundTrip(MsgReAcquire(AcquireTarget.At(Point.Origin)))
    }

    test("round-trip MsgDone") {
        roundTrip(MsgDone)
    }

    // ---------------- golden bytes (cross-checked with the Haskell codec) ----------------

    test("golden: single-tag messages") {
        // [1] / [5] / [7] / [8] / [9] / [10] / [11]
        assert(encHex(MsgAcquired) == "8101")
        assert(encHex(MsgRelease) == "8105")
        assert(encHex(MsgDone) == "8107")
        assert(encHex(MsgAcquire(AcquireTarget.VolatileTip)) == "8108")
        assert(encHex(MsgReAcquire(AcquireTarget.VolatileTip)) == "8109")
        assert(encHex(MsgAcquire(AcquireTarget.ImmutableTip)) == "810a")
        assert(encHex(MsgReAcquire(AcquireTarget.ImmutableTip)) == "810b")
    }

    test("golden: MsgFailure variants") {
        // [2, 0] and [2, 1]
        assert(encHex(MsgFailure(AcquireFailure.PointTooOld)) == "820200")
        assert(encHex(MsgFailure(AcquireFailure.PointNotOnChain)) == "820201")
    }

    test("golden: MsgAcquire SpecificPoint(Origin) — [0, []]") {
        // 82  array(2)
        //   00  uint 0   ; tag
        //   80  array(0) ; Point.Origin
        assert(encHex(MsgAcquire(AcquireTarget.At(Point.Origin))) == "820080")
    }

    test("golden: MsgQuery splices raw query bytes") {
        // body: [3] = 0x81 0x03
        // outer: 82 03 81 03
        val body = ByteString.fromArray(Array[Byte](0x81.toByte, 0x03))
        assert(encHex(MsgQuery(body)) == "82038103")
    }

    test("golden: MsgResult splices raw result bytes") {
        // body: 0x18 0x2a (uint 42)
        // outer: 82 04 18 2a
        val body = ByteString.fromArray(Array[Byte](0x18, 0x2a))
        assert(encHex(MsgResult(body)) == "8204182a")
    }

    // ---------------- malformed input ----------------

    test("decoding rejects unknown tag") {
        // [99]
        val malformed = Array[Byte](0x81.toByte, 0x18, 99)
        given OriginalCborByteArray = OriginalCborByteArray(malformed)
        intercept[Throwable] {
            io.bullet.borer.Cbor.decode(malformed).to[LocalStateQueryMessage].value
        }
    }

    test("decoding rejects wrong arrLen for MsgAcquired") {
        // [1, 0] — Acquired with extra payload
        val malformed = Array[Byte](0x82.toByte, 0x01, 0x00)
        given OriginalCborByteArray = OriginalCborByteArray(malformed)
        intercept[Throwable] {
            io.bullet.borer.Cbor.decode(malformed).to[LocalStateQueryMessage].value
        }
    }

    test("decoding rejects unknown AcquireFailure tag") {
        // [2, 9] — Failure with unknown reason 9
        val malformed = Array[Byte](0x82.toByte, 0x02, 0x09)
        given OriginalCborByteArray = OriginalCborByteArray(malformed)
        intercept[Throwable] {
            io.bullet.borer.Cbor.decode(malformed).to[LocalStateQueryMessage].value
        }
    }
}
