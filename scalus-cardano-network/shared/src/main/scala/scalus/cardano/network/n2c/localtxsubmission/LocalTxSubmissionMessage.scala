package scalus.cardano.network.n2c.localtxsubmission

import io.bullet.borer.{Decoder, Encoder, Reader, Writer}
import scalus.cardano.network.infra.HfcEnvelope
import scalus.uplc.builtin.ByteString

/** LocalTxSubmission mini-protocol messages (protocol id 6), per the ouroboros-network CDDL
  * (`local-tx-submission.cddl`):
  *
  * {{{
  *   localTxSubmissionMsg =
  *     msgSubmitTx /
  *     msgAcceptTx /
  *     msgRejectTx /
  *     msgDone
  *
  *   msgSubmitTx  = [0, transaction]    ; transaction = [eraTag :: word8, tag24(tx-cbor)]
  *   msgAcceptTx  = [1]
  *   msgRejectTx  = [2, reason]         ; reason is era-specific ApplyTxError CBOR
  *   msgDone      = [3]
  * }}}
  *
  * Era-tag scheme matches the rest of the HardForkCombinator wire surface: Byron=0, Shelley=1,
  * Allegra=2, Mary=3, Alonzo=4, Babbage=5, Conway=6, Dijkstra=7, … Newer eras append without
  * renumbering older ones.
  *
  * Typed decomposition of `MsgRejectTx.reason` is **not** in scope for M11 — we surface the raw
  * CBOR bytes plus the era index and let callers diagnose by `toHex`. A typed `ApplyTxError` ADT
  * lands in a follow-up.
  */
sealed trait LocalTxSubmissionMessage

object LocalTxSubmissionMessage {

    /** Initiator → responder: please apply this transaction.
      *
      * @param era
      *   HardForkCombinator era index for the transaction body (Conway = 6 today).
      * @param txBytes
      *   raw CBOR bytes of the era-specific `Transaction`.
      */
    final case class MsgSubmitTx(era: Int, txBytes: ByteString) extends LocalTxSubmissionMessage

    /** Responder → initiator: transaction accepted into the mempool. */
    case object MsgAcceptTx extends LocalTxSubmissionMessage

    /** Responder → initiator: transaction rejected.
      *
      * @param era
      *   the era under which the node attempted to apply the tx — typically equals the era from the
      *   submitting `MsgSubmitTx` but the wire format technically allows the responder to evaluate
      *   at a different era (e.g. just after a hard fork).
      * @param reasonBytes
      *   era-specific `ApplyTxError` CBOR bytes. Decoding into a typed Scala value is deferred to a
      *   later milestone; for now callers diagnose with `toHex`.
      */
    final case class MsgRejectTx(era: Int, reasonBytes: ByteString) extends LocalTxSubmissionMessage

    /** Either side: terminate the protocol. Sent best-effort on shutdown. */
    case object MsgDone extends LocalTxSubmissionMessage

    // ----------------------------------------------------------------------------------------
    // CBOR codec — hand-written, matching the style of `KeepAliveMessage` / `ChainSyncMessage`.
    // ----------------------------------------------------------------------------------------

    given Encoder[LocalTxSubmissionMessage] with
        def write(w: Writer, m: LocalTxSubmissionMessage): Writer = m match {
            case MsgSubmitTx(era, txBytes) =>
                w.writeArrayHeader(2).writeInt(0)
                HfcEnvelope.write(w, era, txBytes)
            case MsgAcceptTx => w.writeArrayHeader(1).writeInt(1)
            case MsgRejectTx(era, reason) =>
                w.writeArrayHeader(2).writeInt(2)
                HfcEnvelope.write(w, era, reason)
            case MsgDone => w.writeArrayHeader(1).writeInt(3)
        }

    given Decoder[LocalTxSubmissionMessage] with
        def read(r: Reader): LocalTxSubmissionMessage = {
            val arrLen = r.readArrayHeader().toInt
            r.readInt() match {
                case 0 if arrLen == 2 =>
                    val (era, bytes) = HfcEnvelope.read(r)
                    MsgSubmitTx(era, bytes)
                case 1 if arrLen == 1 => MsgAcceptTx
                case 2 if arrLen == 2 =>
                    val (era, bytes) = HfcEnvelope.read(r)
                    MsgRejectTx(era, bytes)
                case 3 if arrLen == 1 => MsgDone
                case other =>
                    r.validationFailure(
                      s"unexpected localTxSubmissionMessage tag=$other arrLen=$arrLen"
                    )
            }
        }

}
