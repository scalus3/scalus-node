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
  * }}}
  *
  * Result encodings carry no tagging — the client decodes against the outstanding query type.
  *
  * P1.b ships only `GetChainPoint`; `GetCurrentPParams`, `GetCurrentEra`, `GetUTxOByAddress`,
  * `GetUTxOByTxIn` follow once their per-era result CBOR decoders are in place.
  */
sealed trait LsqQuery[A]

object LsqQuery {

    /** Top-level `GetChainPoint`. Returns the current tip point of the snapshot the client is
      * holding (`[]` for Origin, `[slot, hash]` otherwise — same wire shape as chain-sync).
      */
    case object GetChainPoint extends LsqQuery[Point]

    /** Encode the query body — what the driver splices into `MsgQuery`. */
    given Encoder[LsqQuery[?]] with
        def write(w: Writer, q: LsqQuery[?]): Writer = q match {
            case GetChainPoint => w.writeArrayHeader(1).writeInt(3)
        }

    /** Decode the result bytes for the outstanding query. The match arms narrow `A` per case (Scala
      * 3 GADT pattern matching).
      */
    def decodeResult[A](q: LsqQuery[A], bytes: Array[Byte]): A = q match {
        case GetChainPoint => Cborer.decode(bytes).to[Point].value
    }
}
