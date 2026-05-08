package scalus.cardano.network.n2c.localstatequery

import io.bullet.borer.{DataItem, Decoder, Encoder, Reader, Writer}
import scalus.cardano.ledger.OriginalCborByteArray
import scalus.cardano.network.chainsync.Point
import scalus.uplc.builtin.ByteString

/** LocalStateQuery mini-protocol messages (protocol id 7), per the ouroboros-network CDDL
  * (`local-state-query.cddl`):
  *
  * {{{
  *   localStateQueryMessage =
  *     msgAcquire / msgAcquired / msgFailure / msgQuery /
  *     msgResult / msgRelease / msgReAcquire / lsqMsgDone
  *
  *   msgAcquire   = [0, point] / [8] / [10]   ; SpecificPoint / VolatileTip / ImmutableTip
  *   msgAcquired  = [1]
  *   msgFailure   = [2, failure]              ; failure = 0 (PointTooOld) / 1 (PointNotOnChain)
  *   msgQuery     = [3, query]                ; query is an opaque CBOR item
  *   msgResult    = [4, result]               ; result is an opaque CBOR item
  *   msgRelease   = [5]
  *   msgReAcquire = [6, point] / [9] / [11]   ; SpecificPoint / VolatileTip / ImmutableTip
  *   lsqMsgDone   = [7]
  * }}}
  *
  * The two `Tip` variants in `msgAcquire` (8/10) and `msgReAcquire` (9/11) require the
  * `LocalStateQueryVersion >= V2` negotiated alongside `NodeToClientVersion >= V16`. We require V16
  * unconditionally, so all six variants are always available.
  *
  * `msgQuery`/`msgResult` carry a single arbitrary CBOR item; we surface the raw bytes here and
  * defer typed encode/decode to the driver layer (where the typed `LsqQuery[A]` GADT is in scope).
  * On the encode side we splice via `w.output.writeBytes(...)` — the same workaround the
  * `KeepRaw[A]` codec uses (see [borer issue #761][1] / [#764][2]); on the decode side we capture
  * the slice using cursor positions, which requires an `OriginalCborByteArray` to be in scope.
  * `CborMessageStream` already provides one from its decode buffer.
  *
  * [1]: https://github.com/sirthias/borer/issues/761 [2]:
  * https://github.com/sirthias/borer/issues/764
  */
sealed trait LocalStateQueryMessage

object LocalStateQueryMessage {

    /** Where to acquire a snapshot from. The same target type appears under both `MsgAcquire` and
      * `MsgReAcquire`; the wire encoding's tag depends on the message it sits under (see codec).
      */
    sealed trait AcquireTarget
    object AcquireTarget {

        /** Acquire (or re-acquire) at a specific chain point. */
        final case class At(point: Point) extends AcquireTarget

        /** Acquire (or re-acquire) at the current volatile tip — the chain's current best. */
        case object VolatileTip extends AcquireTarget

        /** Acquire (or re-acquire) at the current immutable tip — the most recent block past the
          * `k`-deep stability horizon. Cheaper for the node to serve large queries from than
          * `VolatileTip` because nothing about the snapshot can be rolled back.
          */
        case object ImmutableTip extends AcquireTarget
    }

    /** Reasons the server may refuse a `MsgAcquire`. Wire-encoded as a single CBOR int. */
    enum AcquireFailure(val tag: Int):
        case PointTooOld extends AcquireFailure(0)
        case PointNotOnChain extends AcquireFailure(1)

    object AcquireFailure {
        def fromTag(tag: Int): Option[AcquireFailure] = tag match {
            case 0 => Some(PointTooOld)
            case 1 => Some(PointNotOnChain)
            case _ => None
        }
    }

    /** Initiator → responder: acquire a snapshot to query against. */
    final case class MsgAcquire(target: AcquireTarget) extends LocalStateQueryMessage

    /** Responder → initiator: snapshot acquired; client may now issue queries. */
    case object MsgAcquired extends LocalStateQueryMessage

    /** Responder → initiator: acquire was rejected. Carries the typed reason. */
    final case class MsgFailure(failure: AcquireFailure) extends LocalStateQueryMessage

    /** Initiator → responder: a typed query, encoded as raw CBOR bytes by the driver before being
      * wrapped here. Decoder layer (in `LocalStateQueryDriver`) holds the type-erased encoder; this
      * codec only handles wire framing.
      */
    final case class MsgQuery(queryBytes: ByteString) extends LocalStateQueryMessage

    /** Responder → initiator: the query's result, captured as raw CBOR for the driver to
      * type-decode against the outstanding query.
      */
    final case class MsgResult(resultBytes: ByteString) extends LocalStateQueryMessage

    /** Initiator → responder: drop the current snapshot; client returns to Idle. */
    case object MsgRelease extends LocalStateQueryMessage

    /** Initiator → responder: release-then-acquire-fresh-snapshot in one round-trip. */
    final case class MsgReAcquire(target: AcquireTarget) extends LocalStateQueryMessage

    /** Either side: terminate the protocol. Sent best-effort on shutdown. */
    case object MsgDone extends LocalStateQueryMessage

    // ----------------------------------------------------------------------------------------
    // CBOR codec — hand-written, matching the style of `LocalTxSubmissionMessage` /
    // `ChainSyncMessage`. Raw-bytes splicing for `MsgQuery`/`MsgResult` follows the
    // `KeepRaw[A]` precedent in `scalus.cardano.ledger`.
    // ----------------------------------------------------------------------------------------

    given Encoder[LocalStateQueryMessage] with
        def write(w: Writer, m: LocalStateQueryMessage): Writer = m match {
            case MsgAcquire(target) =>
                writeAcquire(w, atTag = 0, tipTag = 8, immutableTag = 10, target)
            case MsgAcquired => w.writeArrayHeader(1).writeInt(1)
            case MsgFailure(f) =>
                w.writeArrayHeader(2).writeInt(2).writeInt(f.tag)
            case MsgQuery(bytes) =>
                w.writeArrayHeader(2).writeInt(3)
                w.output.writeBytes(bytes.bytes)
                w
            case MsgResult(bytes) =>
                w.writeArrayHeader(2).writeInt(4)
                w.output.writeBytes(bytes.bytes)
                w
            case MsgRelease => w.writeArrayHeader(1).writeInt(5)
            case MsgReAcquire(target) =>
                writeAcquire(w, atTag = 6, tipTag = 9, immutableTag = 11, target)
            case MsgDone => w.writeArrayHeader(1).writeInt(7)
        }

    given (using OriginalCborByteArray): Decoder[LocalStateQueryMessage] with
        def read(r: Reader): LocalStateQueryMessage = {
            val arrLen = r.readArrayHeader().toInt
            r.readInt() match {
                case 0 if arrLen == 2  => MsgAcquire(AcquireTarget.At(r.read[Point]()))
                case 1 if arrLen == 1  => MsgAcquired
                case 2 if arrLen == 2  => readFailure(r)
                case 3 if arrLen == 2  => MsgQuery(captureRawCbor(r))
                case 4 if arrLen == 2  => MsgResult(captureRawCbor(r))
                case 5 if arrLen == 1  => MsgRelease
                case 6 if arrLen == 2  => MsgReAcquire(AcquireTarget.At(r.read[Point]()))
                case 7 if arrLen == 1  => MsgDone
                case 8 if arrLen == 1  => MsgAcquire(AcquireTarget.VolatileTip)
                case 9 if arrLen == 1  => MsgReAcquire(AcquireTarget.VolatileTip)
                case 10 if arrLen == 1 => MsgAcquire(AcquireTarget.ImmutableTip)
                case 11 if arrLen == 1 => MsgReAcquire(AcquireTarget.ImmutableTip)
                case other =>
                    r.validationFailure(
                      s"unexpected localStateQueryMessage tag=$other arrLen=$arrLen"
                    )
            }
        }

    private def writeAcquire(
        w: Writer,
        atTag: Int,
        tipTag: Int,
        immutableTag: Int,
        target: AcquireTarget
    ): Writer = target match {
        case AcquireTarget.At(point) =>
            w.writeArrayHeader(2).writeInt(atTag)
            w.write(point)
        case AcquireTarget.VolatileTip =>
            w.writeArrayHeader(1).writeInt(tipTag)
        case AcquireTarget.ImmutableTip =>
            w.writeArrayHeader(1).writeInt(immutableTag)
    }

    private def readFailure(r: Reader): MsgFailure = {
        val tag = r.readInt()
        AcquireFailure.fromTag(tag) match {
            case Some(f) => MsgFailure(f)
            case None    => r.validationFailure(s"unknown AcquireFailure tag $tag")
        }
    }

    /** Capture the raw CBOR bytes of the next data item without decoding it. Mirrors the
      * `KeepRaw[A]` decoder shape in `scalus.cardano.ledger` — see
      * https://github.com/sirthias/borer/issues/761#issuecomment-2919035884 for why the two
      * `r.dataItem()` bracketing calls are necessary (cursor only updates on pull).
      *
      * Uses `skipElement` rather than `skipDataItem` because the query/result body is typically a
      * complex CBOR structure (array/map) — `skipDataItem` would only consume the header.
      */
    private def captureRawCbor(r: Reader)(using orig: OriginalCborByteArray): ByteString = {
        r.dataItem()
        val start = r.cursor
        r.skipElement()
        val di = r.dataItem()
        val end = if di == DataItem.EndOfInput then r.input.cursor else r.cursor
        ByteString.fromArray(orig.slice(start.toInt, end.toInt))
    }
}
