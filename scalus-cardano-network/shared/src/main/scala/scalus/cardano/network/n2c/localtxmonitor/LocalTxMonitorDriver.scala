package scalus.cardano.network.n2c.localtxmonitor

import scalus.cardano.infra.CancelToken
import scalus.cardano.ledger.{SlotNo, TransactionHash}
import scalus.cardano.network.infra.{CborMessageStream, MiniProtocolBytes, MiniProtocolId}
import scalus.cardano.network.n2c.localtxmonitor.LocalTxMonitorMessage.*

import scala.concurrent.{ExecutionContext, Future}

/** Initiator-side driver for the LocalTxMonitor mini-protocol (id 9).
  *
  * State machine (CDDL `local-tx-monitor.cddl`, minimal surface):
  *
  * {{{
  *   Idle     ──MsgAcquire────────→ Acquiring
  *            Acquiring ──MsgAcquired(slot)──→ Acquired
  *   Acquired ──MsgHasTx(h)───────→ Querying
  *            Querying  ──MsgRespondHasTx(b)──→ Acquired
  *   Acquired ──MsgRelease────────→ Idle
  *   Idle     ──MsgDone───────────→ Done
  * }}}
  *
  * Only `acquire / hasTx / release / close` are exposed today — enough to back
  * [[scalus.cardano.network.n2c.LocalNodeProvider#checkTransaction]]. `NextTx` and `GetSizes` (CDDL
  * tags 5/6 and 9/10) are deferred and the message ADT does not model them; the decoder rejects
  * those tags so any unexpected server message surfaces loudly.
  *
  * Single in-flight contract: callers MUST await the previous `Future` before issuing the next
  * call. Same single-consumer contract as the sibling
  * [[scalus.cardano.network.n2c.localstatequery.LocalStateQueryDriver]] /
  * [[scalus.cardano.network.n2c.localtxsubmission.LocalTxSubmissionDriver]] — multi-tenant
  * coordination is the caller's job (see [[LocalNodeProvider]]'s `lsqGate` for the existing
  * async-mutex pattern).
  */
final class LocalTxMonitorDriver(
    handle: MiniProtocolBytes,
    cancelToken: CancelToken,
    logger: scribe.Logger = LocalTxMonitorDriver.defaultLogger
)(using ExecutionContext) {

    private val stream =
        new CborMessageStream[LocalTxMonitorMessage](
          MiniProtocolId.LocalTxMonitor,
          handle
        )

    @volatile private var closed: Boolean = false
    // Logical state — `false` = Idle, `true` = Acquired. The two transient states (Acquiring /
    // Querying) live inside the in-flight Future; the single-consumer contract guarantees no
    // observer sees them.
    @volatile private var acquired: Boolean = false

    /** Acquire a mempool snapshot. Returns the slot the snapshot was taken at.
      *
      * Fails the future on:
      *   - already-acquired (caller forgot to release)
      *   - closed driver
      *   - peer EOF mid-acquire
      *   - decode failure / unexpected message in `Acquiring`
      */
    def acquire(): Future[SlotNo] = {
        if closed then return Future.failed(new IllegalStateException("driver closed"))
        if acquired then
            return Future.failed(
              new IllegalStateException("snapshot already acquired — release() first")
            )
        stream
            .send(MsgAcquire, cancelToken)
            .flatMap(_ => stream.receive(cancelToken))
            .flatMap {
                case Some(MsgAcquired(slot)) =>
                    acquired = true
                    Future.successful(slot)
                case Some(other) =>
                    Future.failed(unexpectedMessage("awaiting Acquired", other))
                case None =>
                    Future.failed(
                      new IllegalStateException("peer closed LocalTxMonitor mid-acquire")
                    )
            }
    }

    /** Ask whether `txHash` is present in the held snapshot. Caller must have completed an
      * [[acquire]] before calling.
      */
    def hasTx(era: Int, txHash: TransactionHash): Future[Boolean] = {
        if closed then return Future.failed(new IllegalStateException("driver closed"))
        if !acquired then
            return Future.failed(
              new IllegalStateException("no snapshot acquired; call acquire() first")
            )
        stream
            .send(MsgHasTx(era, txHash), cancelToken)
            .flatMap(_ => stream.receive(cancelToken))
            .flatMap {
                case Some(MsgRespondHasTx(b)) => Future.successful(b)
                case Some(other) => Future.failed(unexpectedMessage("awaiting RespondHasTx", other))
                case None =>
                    Future.failed(
                      new IllegalStateException("peer closed LocalTxMonitor mid-hasTx")
                    )
            }
    }

    /** Release the current snapshot, returning to `Idle`. One-way notification — the server does
      * not reply. Caller may follow with another [[acquire]] or [[close]].
      *
      * The local `acquired` flag is reset before the send so that, on a wire failure mid-release,
      * the next [[acquire]] surfaces the connection error rather than a stale "already acquired"
      * guard.
      */
    def release(): Future[Unit] = {
        if closed then return Future.failed(new IllegalStateException("driver closed"))
        if !acquired then return Future.failed(new IllegalStateException("no snapshot acquired"))
        acquired = false
        stream.send(MsgRelease, cancelToken)
    }

    /** Send `MsgDone` best-effort and mark the driver closed. If a snapshot is currently acquired,
      * sends `MsgRelease` first to reach `Idle` (the only state from which `MsgDone` is valid per
      * the protocol). Both sends are best-effort — failures are logged at debug because the
      * connection is being torn down anyway. Idempotent.
      */
    def close(): Future[Unit] = {
        if closed then return Future.unit
        closed = true
        val toIdle: Future[Unit] =
            if acquired then {
                acquired = false
                stream
                    .send(MsgRelease, CancelToken.never)
                    .recover { case t =>
                        logger.debug(s"MsgRelease best-effort send failed: $t")
                        ()
                    }
            } else Future.unit
        toIdle.flatMap { _ =>
            stream.send(MsgDone, CancelToken.never).recover { case t =>
                logger.debug(s"MsgDone best-effort send failed: $t")
                ()
            }
        }
    }

    /** True when a snapshot is held and queries can be issued. */
    def isAcquired: Boolean = acquired

    private def unexpectedMessage(state: String, msg: LocalTxMonitorMessage): Throwable =
        new IllegalStateException(s"unexpected $msg in state $state")
}

object LocalTxMonitorDriver {

    private val defaultLogger: scribe.Logger =
        scribe.Logger("scalus.cardano.network.n2c.localtxmonitor")
}
