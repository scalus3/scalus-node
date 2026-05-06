package scalus.cardano.network.n2c.localtxsubmission

import scalus.cardano.infra.CancelToken
import scalus.cardano.network.infra.{CborMessageStream, MiniProtocolBytes, MiniProtocolId}
import scalus.cardano.network.n2c.localtxsubmission.LocalTxSubmissionMessage.*
import scalus.uplc.builtin.ByteString

import scala.concurrent.{ExecutionContext, Future}

/** Initiator-side driver for the LocalTxSubmission mini-protocol (id 6).
  *
  * State machine (CDDL `local-tx-submission.cddl`):
  *
  * {{{
  *   Idle ──MsgSubmitTx──→ Busy
  *           Busy ──MsgAcceptTx──→ Idle    (accepted into mempool)
  *           Busy ──MsgRejectTx──→ Idle    (era-specific reason CBOR returned)
  *   Idle ──MsgDone─────→ Done             (best-effort on shutdown)
  * }}}
  *
  * Single in-flight submit at a time: callers MUST await the previous [[submit]] future before
  * issuing the next. Mirrors the same single-consumer contract as the other mini-protocol drivers
  * in this module ([[scalus.cardano.network.chainsync.ChainSyncDriver]] /
  * [[scalus.cardano.network.keepalive.KeepAliveDriver]]) — multi-submitter coordination is the
  * caller's job (the engine's `submit` already serialises via its single worker thread).
  *
  * Wire-protocol surprises (unexpected message in `Busy`, peer EOF mid-submit, decode failure) fail
  * the returned future with a typed cause; the driver does not itself fire any connection-root
  * cancel. Ownership of fatal-fault propagation lives one level up — the provider wiring decides
  * whether a typed reject is fatal (it isn't) and whether a wire-decode failure should bring down
  * the whole connection (it does, via the connection root the caller supplies as `cancelToken`).
  */
final class LocalTxSubmissionDriver(
    handle: MiniProtocolBytes,
    cancelToken: CancelToken,
    logger: scribe.Logger = LocalTxSubmissionDriver.defaultLogger
)(using ExecutionContext) {

    private val stream =
        new CborMessageStream[LocalTxSubmissionMessage](
          MiniProtocolId.LocalTxSubmission,
          handle
        )

    @volatile private var closed: Boolean = false

    /** Submit a transaction. Returns:
      *
      *   - `Right(())` on `MsgAcceptTx` — the node accepted the tx into its mempool. The caller
      *     already has the [[scalus.cardano.ledger.Transaction]] and can derive its hash via
      *     `Transaction.id` if needed.
      *   - `Left(LocalTxSubmissionRejection(era, reasonBytes))` on `MsgRejectTx` — typed
      *     decomposition of the era-specific `ApplyTxError` reason is deferred; callers diagnose by
      *     `reasonBytes.toHex` for now.
      *   - `Future.failed(...)` on wire-protocol surprises (unexpected message, peer EOF, decode
      *     failure, cancel).
      *
      * @param era
      *   HardForkCombinator era index for the encoded tx (Conway = 6). Must match the era the tx
      *   CBOR was encoded under.
      * @param txBytes
      *   raw CBOR bytes of the era-specific `Transaction`.
      */
    def submit(
        era: Int,
        txBytes: ByteString
    ): Future[Either[LocalTxSubmissionRejection, Unit]] = {
        if closed then return Future.failed(new IllegalStateException("driver closed"))
        stream
            .send(MsgSubmitTx(era, txBytes), cancelToken)
            .flatMap(_ => stream.receive(cancelToken))
            .flatMap {
                case Some(MsgAcceptTx) =>
                    Future.successful(Right(()))
                case Some(MsgRejectTx(rejectEra, reasonBytes)) =>
                    Future.successful(Left(LocalTxSubmissionRejection(rejectEra, reasonBytes)))
                case Some(other) =>
                    Future.failed(unexpectedMessage("awaiting accept/reject", other))
                case None =>
                    Future.failed(
                      new IllegalStateException(
                        "peer closed LocalTxSubmission mid-submit; no accept/reject"
                      )
                    )
            }
    }

    /** Send `MsgDone` best-effort and mark the driver closed. Subsequent [[submit]] calls fail
      * synchronously. Idempotent.
      */
    def close(): Future[Unit] = {
        if closed then return Future.unit
        closed = true
        stream.send(MsgDone, CancelToken.never).recover { case t =>
            logger.debug(s"MsgDone best-effort send failed: $t")
            ()
        }
    }

    private def unexpectedMessage(state: String, msg: LocalTxSubmissionMessage): Throwable =
        new IllegalStateException(s"unexpected $msg in state $state")
}

object LocalTxSubmissionDriver {

    private val defaultLogger: scribe.Logger =
        scribe.Logger("scalus.cardano.network.n2c.localtxsubmission")
}

/** Typed wrapper around a `MsgRejectTx` payload. Era-specific decoding of `reasonBytes` into a
  * structured `ApplyTxError` is deferred — callers diagnose with `reasonBytes.toHex` for now.
  */
final case class LocalTxSubmissionRejection(era: Int, reasonBytes: ByteString)
