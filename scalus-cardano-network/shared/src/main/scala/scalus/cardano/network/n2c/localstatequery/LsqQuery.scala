package scalus.cardano.network.n2c.localstatequery

import io.bullet.borer.{Encoder, Writer}
import io.bullet.borer.Cbor as Cborer
import scalus.cardano.address.Address
import scalus.cardano.ledger.{TransactionInput, TransactionOutput, Utxos}
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
  * Result envelope: every `QueryIfCurrent` reply is wrapped in a single-element CBOR array
  * `[<result>]`. The peeling is centralised in [[LsqQuery.QueryIfCurrent]]; top-level queries like
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

    /** Wire tags for the inner `shelleyQuery` element (last array of the QueryIfCurrent envelope).
      * See `Ouroboros.Consensus.Shelley.Ledger.Query` in the consensus repo for the authoritative
      * list; only the ones used here are enumerated.
      */
    private object ShelleyQueryTag {
        val GetCurrentPParams: Int = 3
        val GetUTxOByAddress: Int = 6
        val GetUTxOByTxIn: Int = 15
    }

    /** Borer's auto-derived `Map[K, V]` decoder, given `TransactionInput derives Codec` and the
      * hand-written `given Decoder[TransactionOutput]` in scalus-core.
      */
    private def decodeUtxoMap(bytes: Array[Byte]): Utxos =
        Cborer.decode(bytes).to[Map[TransactionInput, TransactionOutput]].value

    /** Top-level `GetChainPoint`. Returns the current tip point of the snapshot the client is
      * holding (`[]` for Origin, `[slot, hash]` otherwise — same wire shape as chain-sync).
      */
    case object GetChainPoint extends LsqQuery[Point]:
        def write(w: Writer): Writer = w.writeArrayHeader(1).writeInt(3)
        def decode(bytes: Array[Byte]): Point = Cborer.decode(bytes).to[Point].value

    /** Common shape for any `QueryIfCurrent era q` query. Result bytes arrive wrapped in a
      * single-element CBOR array `[<inner>]`; this trait peels the wrap and dispatches to
      * `decodeInner` with the inner bytes.
      *
      * The byte-level peel relies on canonical CBOR — a definite-length array(1) header is the
      * single byte `0x81`. `cardano-node` consensus emits this shape verbatim; an empirical
      * yaci-devkit capture confirmed it (the original `[0,<inner>] / [1,<eraMismatch>]` shape I
      * lifted from older consensus docs is not what V16+ produces).
      *
      * Era-mismatch handling: in modern N2C, the era mismatch path is not exposed via this 1-elem
      * wrap; cardano-node refuses incompatible-era queries earlier in the protocol (mini-protocol
      * close or `MsgFailure` on acquire). If we ever see a wrap that isn't `0x81 <inner>`, we raise
      * an IllegalArgumentException so the surprise surfaces loudly rather than corrupting.
      */
    sealed trait QueryIfCurrent[A] extends LsqQuery[A]:
        def era: Int

        /** Splice the shelleyQuery portion (the innermost array). The outer `[0, [0, [era, ...]]]`
          * wrap is centralised in [[write]].
          */
        protected def writeShelleyQuery(w: Writer): Writer

        protected def decodeInner(bytes: Array[Byte]): A

        final def write(w: Writer): Writer = {
            w.writeArrayHeader(2).writeInt(0)
            w.writeArrayHeader(2).writeInt(0)
            w.writeArrayHeader(2).writeInt(era)
            writeShelleyQuery(w)
        }

        final def decode(bytes: Array[Byte]): A = {
            if bytes.length < 1 || bytes(0) != 0x81.toByte then
                throw new IllegalArgumentException(
                  "LSQ QueryIfCurrent envelope: expected CBOR array(1) header (0x81) at byte 0; " +
                      f"got 0x${bytes.headOption.map(_ & 0xff).getOrElse(0)}%02x"
                )
            decodeInner(java.util.Arrays.copyOfRange(bytes, 1, bytes.length))
        }

    /** Per-era `GetCurrentPParams` — wraps as `QueryIfCurrent era (ShelleyQuery 3)`. The result
      * decoder is injected because each era's PParams CBOR is decoded by its own scalus class (e.g.
      * [[scalus.cardano.ledger.ConwayProtocolParams]] for Conway), several of which are JVM-only.
      */
    final case class GetCurrentPParams[A](era: Int, decoder: Array[Byte] => A)
        extends QueryIfCurrent[A]:
        protected def writeShelleyQuery(w: Writer): Writer =
            w.writeArrayHeader(1).writeInt(ShelleyQueryTag.GetCurrentPParams)
        protected def decodeInner(bytes: Array[Byte]): A = decoder(bytes)

    /** `QueryIfCurrent era (GetUTxOByAddress addrs)` — UTxOs at any of the given addresses. The
      * address set is wire-encoded as a bare CBOR array (per `encodeFoldable` in consensus); NO
      * tag-258 prefix — the Conway tag-258 set convention applies to serialized state/transactions,
      * not to LSQ query parameters, which cardano-node refuses outright with
      * `DeserialiseFailure "expected list len or indef"`.
      *
      * Result CBOR is the bare `Map[TransactionInput, TransactionOutput]` produced by
      * `cardano-ledger`'s `EncCBOR (UTxO era)`; borer auto-derives the Map decoder from the per-key
      * and per-value givens already in scope (`TransactionInput derives Codec`,
      * `given Decoder[TransactionOutput]`).
      */
    final case class GetUTxOByAddress(era: Int, addresses: Set[Address])
        extends QueryIfCurrent[Utxos]:
        protected def writeShelleyQuery(w: Writer): Writer = {
            w.writeArrayHeader(2).writeInt(ShelleyQueryTag.GetUTxOByAddress)
            writeBareSet(w, addresses)
        }
        protected def decodeInner(bytes: Array[Byte]): Utxos = decodeUtxoMap(bytes)

    /** `QueryIfCurrent era (GetUTxOByTxIn inputs)` — UTxOs at any of the given transaction inputs.
      * Wire shape parallels [[GetUTxOByAddress]]; result has the same shape.
      */
    final case class GetUTxOByTxIn(era: Int, inputs: Set[TransactionInput])
        extends QueryIfCurrent[Utxos]:
        protected def writeShelleyQuery(w: Writer): Writer = {
            w.writeArrayHeader(2).writeInt(ShelleyQueryTag.GetUTxOByTxIn)
            writeBareSet(w, inputs)
        }
        protected def decodeInner(bytes: Array[Byte]): Utxos = decodeUtxoMap(bytes)

    private def writeBareSet[A: io.bullet.borer.Encoder](w: Writer, s: Iterable[A]): Writer = {
        val seq = IndexedSeq.from(s)
        w.writeArrayHeader(seq.size)
        seq.foreach(w.write(_))
        w
    }

    /** Single shared encoder — every case dispatches through its own `write`. */
    given Encoder[LsqQuery[?]] with
        def write(w: Writer, q: LsqQuery[?]): Writer = q.write(w)
}
