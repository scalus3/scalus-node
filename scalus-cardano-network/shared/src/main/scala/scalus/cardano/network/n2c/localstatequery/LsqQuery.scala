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

    /** Per-era `GetCurrentPParams` — wraps as `QueryIfCurrent era (ShelleyQuery 3)`. The result
      * decoder is injected because each era's PParams CBOR is decoded by its own scalus class (e.g.
      * [[scalus.cardano.ledger.ConwayProtocolParams]] for Conway), several of which are JVM-only.
      */
    final case class GetCurrentPParams[A](era: Int, decoder: Array[Byte] => A) extends LsqQuery[A]:
        def write(w: Writer): Writer =
            // [0, [0, [era, [3]]]]
            //   ^   ^   ^    ^
            //   |   |   |    shelleyQuery: GetCurrentPParams
            //   |   |   era-dispatch
            //   |   BlockQuery: QueryIfCurrent
            //   top: BlockQuery
            w.writeArrayHeader(2).writeInt(0)
            w.writeArrayHeader(2).writeInt(0)
            w.writeArrayHeader(2).writeInt(era)
            w.writeArrayHeader(1).writeInt(3)
        def decode(bytes: Array[Byte]): A = decoder(bytes)

    /** Single shared encoder — every case dispatches through its own `write`. */
    given Encoder[LsqQuery[?]] with
        def write(w: Writer, q: LsqQuery[?]): Writer = q.write(w)
}
