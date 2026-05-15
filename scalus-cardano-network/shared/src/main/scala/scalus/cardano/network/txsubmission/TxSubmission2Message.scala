package scalus.cardano.network.txsubmission

import io.bullet.borer.{Decoder, Encoder, Reader, Writer}
import scalus.cardano.network.infra.HfcEnvelope
import scalus.uplc.builtin.ByteString

/** TxSubmission2 mini-protocol messages (protocol id 4), per the ouroboros-network CDDL.
  *
  * The peer (cardano-node) is the *consumer*; we (the client) are the *producer / responder*. The
  * peer drives the conversation by sending `MsgRequestTxIds` / `MsgRequestTxs` and we respond with
  * the matching `MsgReplyTxIds` / `MsgReplyTxs`. There is **no** `MsgRejectTx` — the protocol has
  * no peer→client acceptance/rejection signal. See `n2n-txsubmission2-m8.md` § *Submit semantics*
  * for the consequences.
  *
  * {{{
  *   txSubmission2Message =
  *       msgInit          ; [6]
  *     / msgRequestTxIds  ; [0, blocking, numAck, numReq]
  *     / msgReplyTxIds    ; [1, [* [txId, sizeBytes]]]
  *     / msgRequestTxs    ; [2, [* txId]]
  *     / msgReplyTxs      ; [3, [* tx]]
  *     / msgDone          ; [4]
  *
  *   blocking  = bool
  *   numAck    = uint .size 2   ; word16
  *   numReq    = uint .size 2   ; word16
  *   sizeBytes = uint .size 4   ; word32
  * }}}
  *
  * Tx-id / tx wire shape (HardForkCombinator-tagged, matching `txId.cddl` / `tx.cddl`):
  *   - `txId = [era, hashBytes]` — `hashBytes` is the 32-byte tx hash for Shelley-and-later eras
  *     (Byron has a different layout — out of scope here).
  *   - `tx   = [era, #6.24(bytes .cbor era_transaction)]` — the era-specific transaction CBOR
  *     wrapped in CBOR tag 24 (EmbeddedCBOR), same envelope shape as
  *     [[scalus.cardano.network.n2c.localtxsubmission.LocalTxSubmissionMessage]]'s `MsgSubmitTx`.
  *
  * **Era numbering caveat.** The CDDL fragment `txId.cddl` in
  * `ouroboros-consensus/.../node-to-node/txsubmission2/` lists eras in an order with Babbage at
  * position 7 ("// TODO(geo2a): why are this order? Why babbage is last?") while the sibling
  * `tx.cddl` uses the standard Byron(0)..Dijkstra(7) order. This module uses the standard
  * HardForkCombinator numbering — same convention as `LocalTxSubmissionMessage` — and relies on the
  * yaci-devkit IT (M8 Phase 3) to confirm against a real node. If the yaci IT surfaces a mismatch
  * on the txId side, the fix lives in a small mapping function applied only to [[MsgReplyTxIds]] /
  * [[MsgRequestTxs]] encoding.
  */
sealed trait TxSubmission2Message

object TxSubmission2Message {

    /** Client → server: start of the protocol. Sent once, immediately after handshake. */
    case object MsgInit extends TxSubmission2Message

    /** Server → client: "I've acknowledged `numAck` of the ids you previously offered; please give
      * me up to `numReq` new ids; if `blocking = true` and you have none, wait until you do."
      */
    final case class MsgRequestTxIds(
        blocking: Boolean,
        numAck: Int,
        numReq: Int
    ) extends TxSubmission2Message {
        require(numAck >= 0 && numAck <= 0xffff, s"numAck must be u16, got $numAck")
        require(numReq >= 0 && numReq <= 0xffff, s"numReq must be u16, got $numReq")
    }

    /** Client → server: a list of `(txid, sizeBytes)` pairs offered for the peer to pull. Order is
      * preserved — `numAck` in the next [[MsgRequestTxIds]] consumes from the front.
      */
    final case class MsgReplyTxIds(txIds: Seq[(TxId, Long)]) extends TxSubmission2Message

    /** Server → client: please send the bodies for these txids. */
    final case class MsgRequestTxs(txIds: Seq[TxId]) extends TxSubmission2Message

    /** Client → server: a list of tx bodies, in the order the server requested them. */
    final case class MsgReplyTxs(txs: Seq[TxBody]) extends TxSubmission2Message

    /** Either side: terminate the protocol. Sent best-effort on shutdown. */
    case object MsgDone extends TxSubmission2Message

    /** An era-tagged transaction id — wire shape `[era, hashBytes]`. `era` follows the
      * HardForkCombinator numbering (Byron=0, Shelley=1, …, Conway=6, Dijkstra=7); `hashBytes` is
      * the 32-byte hash for Shelley-and-later eras.
      */
    final case class TxId(era: Int, hashBytes: ByteString)

    /** An era-tagged transaction body — wire shape `[era, #6.24(bytes .cbor transaction)]`. */
    final case class TxBody(era: Int, txBytes: ByteString)

    // ----------------------------------------------------------------------------------------
    // CBOR codec — hand-written, matching the style of `KeepAliveMessage` /
    // `LocalTxSubmissionMessage` / `BlockFetchMessage`.
    // ----------------------------------------------------------------------------------------

    given Encoder[TxSubmission2Message] with
        def write(w: Writer, m: TxSubmission2Message): Writer = m match {
            case MsgInit =>
                w.writeArrayHeader(1).writeInt(6)
            case MsgRequestTxIds(blocking, numAck, numReq) =>
                w.writeArrayHeader(4)
                    .writeInt(0)
                    .writeBoolean(blocking)
                    .writeInt(numAck)
                    .writeInt(numReq)
            case MsgReplyTxIds(pairs) =>
                w.writeArrayHeader(2).writeInt(1).writeArrayHeader(pairs.size)
                pairs.foreach { case (txid, size) =>
                    w.writeArrayHeader(2)
                    writeTxId(w, txid)
                    w.writeLong(size)
                }
                w
            case MsgRequestTxs(ids) =>
                w.writeArrayHeader(2).writeInt(2).writeArrayHeader(ids.size)
                ids.foreach(writeTxId(w, _))
                w
            case MsgReplyTxs(txs) =>
                w.writeArrayHeader(2).writeInt(3).writeArrayHeader(txs.size)
                txs.foreach(writeTxBody(w, _))
                w
            case MsgDone =>
                w.writeArrayHeader(1).writeInt(4)
        }

    given Decoder[TxSubmission2Message] with
        def read(r: Reader): TxSubmission2Message = {
            val arrLen = r.readArrayHeader().toInt
            r.readInt() match {
                case 0 if arrLen == 4 =>
                    val blocking = r.readBoolean()
                    val numAck = r.readInt()
                    val numReq = r.readInt()
                    MsgRequestTxIds(blocking, numAck, numReq)
                case 1 if arrLen == 2 =>
                    val n = r.readArrayHeader().toInt
                    val b = Seq.newBuilder[(TxId, Long)]
                    var i = 0
                    while i < n do {
                        val pairLen = r.readArrayHeader().toInt
                        if pairLen != 2 then
                            r.validationFailure(s"MsgReplyTxIds pair arrLen=$pairLen (expected 2)")
                        val txid = readTxId(r)
                        val size = r.readLong()
                        b += (txid -> size)
                        i += 1
                    }
                    MsgReplyTxIds(b.result())
                case 2 if arrLen == 2 =>
                    val n = r.readArrayHeader().toInt
                    val b = Seq.newBuilder[TxId]
                    var i = 0
                    while i < n do { b += readTxId(r); i += 1 }
                    MsgRequestTxs(b.result())
                case 3 if arrLen == 2 =>
                    val n = r.readArrayHeader().toInt
                    val b = Seq.newBuilder[TxBody]
                    var i = 0
                    while i < n do { b += readTxBody(r); i += 1 }
                    MsgReplyTxs(b.result())
                case 4 if arrLen == 1 => MsgDone
                case 6 if arrLen == 1 => MsgInit
                case other =>
                    r.validationFailure(
                      s"unexpected txSubmission2Message tag=$other arrLen=$arrLen"
                    )
            }
        }

    private def writeTxId(w: Writer, txid: TxId): Writer =
        w.writeArrayHeader(2).writeInt(txid.era).writeBytes(txid.hashBytes.bytes)

    private def readTxId(r: Reader): TxId = {
        val len = r.readArrayHeader().toInt
        if len != 2 then r.validationFailure(s"TxId arrLen=$len (expected 2)")
        val era = r.readInt()
        val hash = ByteString.fromArray(r.readByteArray())
        TxId(era, hash)
    }

    private def writeTxBody(w: Writer, body: TxBody): Writer =
        HfcEnvelope.write(w, body.era, body.txBytes)

    private def readTxBody(r: Reader): TxBody = {
        val (era, bytes) = HfcEnvelope.read(r)
        TxBody(era, bytes)
    }
}
