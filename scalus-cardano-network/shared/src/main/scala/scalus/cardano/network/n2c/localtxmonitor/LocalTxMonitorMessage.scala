package scalus.cardano.network.n2c.localtxmonitor

import io.bullet.borer.{Decoder, Encoder, Reader, Writer}
import scalus.cardano.ledger.{SlotNo, TransactionHash}

/** LocalTxMonitor mini-protocol messages (protocol id 9), per the ouroboros-network CDDL
  * (`local-tx-monitor.cddl`):
  *
  * {{{
  *   localTxMonitorMessage
  *     = msgDone           ; [0]
  *     / msgAcquire        ; [1]
  *     / msgAcquired       ; [2, slotNo]
  *     / msgRelease        ; [3]
  *     / msgNextTx         ; [5]                ; deferred
  *     / msgRespondNextTx  ; [6] / [6, tx]      ; deferred
  *     / msgHasTx          ; [7, txId]
  *     / msgRespondHasTx   ; [8, bool]
  *     / msgGetSizes       ; [9]                ; deferred
  *     / msgRespondGetSizes; [10, sizes]        ; deferred
  *
  *   slotNo = uint
  *   txId   = bytes .size 32
  * }}}
  *
  * Minimal surface for [[scalus.cardano.network.n2c.LocalNodeProvider#checkTransaction]]: acquire a
  * mempool snapshot, ask `HasTx`, release. `NextTx` and `GetSizes` aren't modelled today; the
  * decoder rejects those tags so any unexpected server message surfaces loudly.
  */
sealed trait LocalTxMonitorMessage

object LocalTxMonitorMessage {

    /** Initiator → responder: terminate the protocol. Sent best-effort on shutdown. */
    case object MsgDone extends LocalTxMonitorMessage

    /** Initiator → responder: acquire a mempool snapshot. */
    case object MsgAcquire extends LocalTxMonitorMessage

    /** Responder → initiator: snapshot acquired at the given slot. */
    final case class MsgAcquired(slot: SlotNo) extends LocalTxMonitorMessage

    /** Initiator → responder: drop the current snapshot; client returns to Idle. */
    case object MsgRelease extends LocalTxMonitorMessage

    /** Initiator → responder: is the transaction with this hash present in the held snapshot? */
    final case class MsgHasTx(txHash: TransactionHash) extends LocalTxMonitorMessage

    /** Responder → initiator: yes / no. */
    final case class MsgRespondHasTx(present: Boolean) extends LocalTxMonitorMessage

    // ----------------------------------------------------------------------------------------
    // CBOR codec — hand-written, matching the style of `LocalStateQueryMessage` /
    // `LocalTxSubmissionMessage`.
    // ----------------------------------------------------------------------------------------

    given Encoder[LocalTxMonitorMessage] with
        def write(w: Writer, m: LocalTxMonitorMessage): Writer = m match {
            case MsgDone           => w.writeArrayHeader(1).writeInt(0)
            case MsgAcquire        => w.writeArrayHeader(1).writeInt(1)
            case MsgAcquired(slot) => w.writeArrayHeader(2).writeInt(2).writeLong(slot)
            case MsgRelease        => w.writeArrayHeader(1).writeInt(3)
            case MsgHasTx(txHash) =>
                w.writeArrayHeader(2).writeInt(7)
                w.writeBytes(txHash.bytes)
            case MsgRespondHasTx(b) =>
                w.writeArrayHeader(2).writeInt(8).writeBoolean(b)
        }

    given Decoder[LocalTxMonitorMessage] with
        def read(r: Reader): LocalTxMonitorMessage = {
            val arrLen = r.readArrayHeader().toInt
            r.readInt() match {
                case 0 if arrLen == 1 => MsgDone
                case 1 if arrLen == 1 => MsgAcquire
                case 2 if arrLen == 2 => MsgAcquired(r.readLong())
                case 3 if arrLen == 1 => MsgRelease
                case 7 if arrLen == 2 =>
                    MsgHasTx(TransactionHash.fromArray(r.readBytes[Array[Byte]]()))
                case 8 if arrLen == 2 => MsgRespondHasTx(r.readBoolean())
                case other =>
                    r.validationFailure(
                      s"unexpected localTxMonitorMessage tag=$other arrLen=$arrLen"
                    )
            }
        }
}
