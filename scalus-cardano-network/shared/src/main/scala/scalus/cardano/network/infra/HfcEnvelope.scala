package scalus.cardano.network.infra

import io.bullet.borer.{Reader, Tag, Writer}
import scalus.uplc.builtin.ByteString

/** HardForkCombinator era envelope shared by the wire-level codecs that carry an era-tagged
  * transaction (or era-tagged opaque CBOR) payload. Shape:
  *
  * {{{
  *   envelope = [era :: uint, #6.24(bytes .cbor era_payload)]
  * }}}
  *
  * Used by `LocalTxSubmissionMessage.MsgSubmitTx` / `MsgRejectTx` and by
  * `TxSubmission2Message.TxBody`. `era` follows the standard HardForkCombinator numbering (Byron=0,
  * Shelley=1, …, Conway=6, Dijkstra=7).
  */
object HfcEnvelope {

    /** Write `[era, tag24(bytes)]`. */
    def write(w: Writer, era: Int, bytes: ByteString): Writer =
        w.writeArrayHeader(2).writeInt(era).writeTag(Tag.EmbeddedCBOR).writeBytes(bytes.bytes)

    /** Read `[era, tag24(bytes)]`. */
    def read(r: Reader): (Int, ByteString) = {
        val len = r.readArrayHeader().toInt
        if len != 2 then r.validationFailure(s"unexpected HFC envelope arrLen=$len (expected 2)")
        val era = r.readInt()
        r.readTag() match {
            case Tag.EmbeddedCBOR => ()
            case other =>
                r.validationFailure(
                  s"expected CBOR tag 24 (EmbeddedCBOR) for HFC envelope inner, got $other"
                )
        }
        val bytes = ByteString.fromArray(r.readByteArray())
        (era, bytes)
    }
}
