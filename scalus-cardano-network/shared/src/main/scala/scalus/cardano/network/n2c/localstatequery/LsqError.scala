package scalus.cardano.network.n2c.localstatequery

import scalus.cardano.network.n2c.localstatequery.LocalStateQueryMessage.AcquireFailure

/** Recoverable, protocol-level error surfaced by [[LocalStateQueryDriver.acquire]] /
  * [[LocalStateQueryDriver.query]] / [[LsqQuery.decode]]. Distinct from connection-level faults
  * (peer EOF, unexpected message, driver closed), which the driver surfaces via `Future.failed`
  * because the only sensible reaction is to tear the connection down.
  *
  * Each case is itself a `RuntimeException`, matching the convention of sibling error ADTs in this
  * module ([[scalus.cardano.network.keepalive.KeepAliveError]],
  * [[scalus.cardano.network.n2c.handshake.HandshakeError]]). Callers can either pattern-match on
  * the typed `Left(_: LsqError)` or treat the value as a `Throwable`.
  */
sealed abstract class LsqError(message: String) extends RuntimeException(message)

object LsqError {

    /** `MsgFailure` reply to an `MsgAcquire` — the snapshot the client asked for isn't available.
      * Typical reaction: drop a stale `SpecificPoint` and acquire `VolatileTip`.
      */
    final case class AcquireRejected(failure: AcquireFailure)
        extends LsqError(s"LSQ acquire rejected: $failure")

    /** `QueryIfCurrent` was issued at era `expected.eraIdx` but the node's current era is
      * `actual.eraIdx`. Typical reaction: retry with the actual era, or fall back to a backup that
      * isn't era-parameterised (e.g. `BackupSource.Blockfrost`).
      */
    final case class EraMismatch(expected: EraInfo, actual: EraInfo)
        extends LsqError(
          s"LSQ era mismatch: queried era ${expected.eraIdx} (${expected.eraName}), " +
              s"node at era ${actual.eraIdx} (${actual.eraName})"
        )

    /** The query result CBOR could not be decoded — either our per-era decoder disagrees with the
      * node's encoding (rare, indicates codec drift) or the wire payload is malformed.
      */
    final case class DecodeFailure(reason: String) extends LsqError(s"LSQ decode failure: $reason")

    /** Reserved for a future Byron-era guard — not produced by the current driver because every
      * `QueryIfCurrent` carries an explicit `era: Int` and the Byron era only supports
      * `GetUpdateInterfaceState`, which we don't expose. Kept in the ADT so a `case _` default in
      * future query result codecs doesn't silently widen.
      */
    final case class UnsupportedEra(eraTag: Int) extends LsqError(s"LSQ unsupported era: $eraTag")
}
