package scalus.cardano.network.n2c.localstatequery

import io.bullet.borer.{Encoder, Writer}
import io.bullet.borer.Cbor as Cborer
import scalus.cardano.network.chainsync.Point

/** Typed `LocalStateQuery` queries (top-level / HFC-wrapped / per-era).
  *
  * Wire encoding follows the layered structure documented in the consensus repo's
  * `docs/website/contents/explanations/queries.md`:
  *
  * {{{
  *   top-level Query  =  [0, <BlockQuery>]      ; BlockQuery
  *                    /  [3]                    ; GetChainPoint  (used here)
  *                    /  [1] / [2]              ; GetSystemStart / GetChainBlockNo
  *
  *   BlockQuery       =  [0, <era-dispatch>]    ; QueryIfCurrent
  *                    /  [1, [0], <era>]        ; QueryAnytime GetEraStart era
  *                    /  [2, [0]] / [2, [1]]    ; QueryHardFork GetInterpreter / GetCurrentEra
  *
  *   era-dispatch     =  [0, 0]                 ; Byron  (only GetUpdateInterfaceState)
  *                    /  [N, <shelleyQuery>]    ; Shelley=1 .. Conway=6
  *
  *   shelleyQuery     =  [3]                    ; GetCurrentPParams  (used here)
  *                    /  [0] / [1] / ...        ; GetLedgerTip / GetEpochNo / ...
  * }}}
  *
  * Result envelope: every `QueryIfCurrent` reply is wrapped in `[0, <result>]` (current era) or
  * `[1, <eraMismatch>]` (the server is now in a different era than the client asked for). The
  * peeling is centralised in [[LsqQuery.QueryIfCurrent]]; top-level queries like
  * [[LsqQuery.GetChainPoint]] sit outside it.
  *
  * Each case provides its own encode/decode — keeps this module independent of any specific era's
  * result-CBOR shape, which often lives in platform-specific scalus modules (e.g.
  * [[scalus.cardano.ledger.ConwayProtocolParams]] is JVM-only).
  */
sealed trait LsqQuery[A]:
    /** Splice the query body into `MsgQuery`. */
    def write(w: Writer): Writer

    /** Decode the result CBOR (the opaque payload of `MsgResult`) into a typed value. */
    def decode(bytes: Array[Byte]): A

object LsqQuery {

    /** Top-level `GetChainPoint`. Returns the current tip point of the snapshot the client is
      * holding (`[]` for Origin, `[slot, hash]` otherwise — same wire shape as chain-sync).
      */
    case object GetChainPoint extends LsqQuery[Point]:
        def write(w: Writer): Writer = w.writeArrayHeader(1).writeInt(3)
        def decode(bytes: Array[Byte]): Point = Cborer.decode(bytes).to[Point].value

    /** Common shape for any `QueryIfCurrent era q` query. Result bytes arrive wrapped in
      * `[0, <inner>] / [1, <eraMismatch>]`; this trait peels the envelope and dispatches to
      * `decodeInner` on success, throws [[LsqEraMismatchException]] on mismatch.
      *
      * The byte-level peel relies on canonical CBOR (definite-length array(2) header `0x82` and
      * single-byte ints `0x00`/`0x01` for tags 0/1) — which is what `cardano-node` consensus emits
      * for query responses. If the node ever switches to non-canonical encodings the peel would
      * miscount; the assertions guard against silent corruption.
      */
    sealed trait QueryIfCurrent[A] extends LsqQuery[A]:
        def era: Int
        protected def decodeInner(bytes: Array[Byte]): A

        final def decode(bytes: Array[Byte]): A = {
            if bytes.length < 2 || bytes(0) != 0x82.toByte then
                throw new IllegalArgumentException(
                  "LSQ QueryIfCurrent envelope: expected CBOR array(2) header (0x82) at byte 0; " +
                      s"got 0x${bytes.headOption.map(_ & 0xff).getOrElse(0).toHexString}"
                )
            bytes(1) match {
                case 0x00 =>
                    decodeInner(java.util.Arrays.copyOfRange(bytes, 2, bytes.length))
                case 0x01 =>
                    throw new LsqEraMismatchException(
                      queriedEra = era,
                      mismatchCbor = java.util.Arrays.copyOfRange(bytes, 2, bytes.length)
                    )
                case other =>
                    throw new IllegalArgumentException(
                      s"LSQ QueryIfCurrent envelope: tag must be 0 or 1, got 0x${(other & 0xff).toHexString}"
                    )
            }
        }

    /** Per-era `GetCurrentPParams` — wraps as `QueryIfCurrent era (ShelleyQuery 3)`. The result
      * decoder is injected because each era's PParams CBOR is decoded by its own scalus class (e.g.
      * [[scalus.cardano.ledger.ConwayProtocolParams]] for Conway), several of which are JVM-only.
      */
    final case class GetCurrentPParams[A](era: Int, decoder: Array[Byte] => A)
        extends QueryIfCurrent[A]:
        def write(w: Writer): Writer =
            w.writeArrayHeader(2).writeInt(0)
            w.writeArrayHeader(2).writeInt(0)
            w.writeArrayHeader(2).writeInt(era)
            w.writeArrayHeader(1).writeInt(3)
        protected def decodeInner(bytes: Array[Byte]): A = decoder(bytes)

    /** Single shared encoder — every case dispatches through its own `write`. */
    given Encoder[LsqQuery[?]] with
        def write(w: Writer, q: LsqQuery[?]): Writer = q.write(w)
}

/** Raised when a `QueryIfCurrent` returns `[1, eraMismatch]` — the client asked the era is no
  * longer current on the node side. Carries the queried era and the raw mismatch CBOR for
  * diagnostics (decoding the mismatch payload itself is era-set-dependent and deferred).
  */
final class LsqEraMismatchException(val queriedEra: Int, val mismatchCbor: Array[Byte])
    extends RuntimeException(
      s"LSQ era mismatch: queried era=$queriedEra; node responded with eraMismatch payload " +
          s"(${mismatchCbor.length} bytes)"
    )
