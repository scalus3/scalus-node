package scalus.cardano.network.n2c.localstatequery

import scalus.cardano.infra.CancelToken
import scalus.cardano.network.infra.{CborMessageStream, MiniProtocolBytes, MiniProtocolId}
import scalus.cardano.network.n2c.localstatequery.LocalStateQueryMessage.*
import scalus.serialization.cbor.Cbor
import scalus.uplc.builtin.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Initiator-side driver for the LocalStateQuery mini-protocol (id 7).
  *
  * State machine (CDDL `local-state-query.cddl`, with V2 ImmutableTip support):
  *
  * {{{
  *   Idle      ──MsgAcquire(target)────→ Acquiring
  *             Acquiring ──MsgAcquired──→ Acquired
  *             Acquiring ──MsgFailure──→ Idle    (snapshot rejected)
  *   Acquired  ──MsgQuery─────────────→ Querying
  *             Querying  ──MsgResult──→ Acquired
  *   Acquired  ──MsgRelease───────────→ Idle
  *   Acquired  ──MsgReAcquire(target)─→ Acquiring
  *   Idle      ──MsgDone──────────────→ Done           (best-effort on shutdown)
  * }}}
  *
  * Today this driver implements `acquire` / `release` / `close`; `query[A]` and `reAcquire` land
  * alongside the `LsqQuery` GADT in a follow-up.
  *
  * Single in-flight contract: callers MUST await the previous `Future` before issuing the next
  * call. Mirrors the same single-consumer contract as
  * [[scalus.cardano.network.n2c.localtxsubmission.LocalTxSubmissionDriver]] /
  * [[scalus.cardano.network.chainsync.ChainSyncDriver]] — multi-tenant coordination is the caller's
  * job.
  *
  * Wire-protocol surprises (unexpected message for the current state, peer EOF mid-acquire, decode
  * failure) fail the returned future with a typed cause; the driver does not itself fire the
  * connection-root cancel. Ownership of fatal-fault propagation lives one level up — the provider
  * decides whether a wire-decode failure should bring down the connection (it does, via the
  * connection root the caller supplies as `cancelToken`).
  */
final class LocalStateQueryDriver(
    handle: MiniProtocolBytes,
    cancelToken: CancelToken,
    logger: scribe.Logger = LocalStateQueryDriver.defaultLogger
)(using ExecutionContext) {

    private val stream =
        new CborMessageStream[LocalStateQueryMessage](
          MiniProtocolId.LocalStateQuery,
          handle
        )

    @volatile private var closed: Boolean = false
    // Logical state — `false` = Idle (no snapshot held), `true` = Acquired. The two transient
    // states (Acquiring / Querying) live inside the in-flight Future so we don't need to model
    // them here; the single-consumer contract guarantees no observer sees them.
    @volatile private var acquired: Boolean = false

    /** Acquire a snapshot at `target`. The returned future completes once the server replies.
      *
      *   - `Right(())` on `MsgAcquired` — driver is now in `Acquired`; queries can be issued.
      *   - `Left(failure)` on `MsgFailure` — driver stays in `Idle`; caller may try a different
      *     target (typical recovery: drop a stale `SpecificPoint` and acquire `VolatileTip`).
      *   - `Future.failed(...)` on wire-protocol surprises (unexpected message, peer EOF, decode
      *     failure, cancel).
      */
    def acquire(target: AcquireTarget): Future[Either[AcquireFailure, Unit]] = {
        if closed then return Future.failed(new IllegalStateException("driver closed"))
        if acquired then
            return Future.failed(
              new IllegalStateException(
                "snapshot already acquired — release() first, or use reAcquire() once it lands"
              )
            )
        stream
            .send(MsgAcquire(target), cancelToken)
            .flatMap(_ => stream.receive(cancelToken))
            .flatMap {
                case Some(MsgAcquired) =>
                    acquired = true
                    Future.successful(Right(()))
                case Some(MsgFailure(f)) =>
                    // server rejected — we stay in Idle
                    Future.successful(Left(f))
                case Some(other) =>
                    Future.failed(unexpectedMessage("awaiting Acquired/Failure", other))
                case None =>
                    Future.failed(
                      new IllegalStateException("peer closed LSQ mid-acquire; no Acquired/Failure")
                    )
            }
    }

    /** Issue a typed query against the held snapshot. Caller must have completed an [[acquire]]
      * (`Right(())`) before calling.
      *
      * The future fails on:
      *   - `IllegalStateException` if the driver is closed or no snapshot is held
      *   - `IllegalStateException` if the peer sends an unexpected message in `Querying`
      *   - whatever the per-query result decoder raises on malformed result bytes
      *   - `IllegalStateException` if the peer EOFs mid-query
      */
    def query[A](q: LsqQuery[A]): Future[A] = {
        if closed then return Future.failed(new IllegalStateException("driver closed"))
        if !acquired then
            return Future.failed(
              new IllegalStateException("no snapshot acquired; call acquire() first")
            )
        val queryBytes = ByteString.fromArray(Cbor.encode(q: LsqQuery[?]))
        stream
            .send(MsgQuery(queryBytes), cancelToken)
            .flatMap(_ => stream.receive(cancelToken))
            .flatMap {
                case Some(MsgResult(resultBytes)) =>
                    try Future.successful(q.decode(resultBytes.bytes))
                    catch case NonFatal(t) => Future.failed(t)
                case Some(other) =>
                    Future.failed(unexpectedMessage("awaiting Result", other))
                case None =>
                    Future.failed(new IllegalStateException("peer closed LSQ mid-query"))
            }
    }

    /** Release the current snapshot, returning to `Idle`. One-way notification — the server does
      * not reply. Caller may follow with another [[acquire]] or [[close]].
      *
      * Local `acquired` flag is reset before the send so that, on a wire failure mid-release, the
      * next [[acquire]] surfaces the connection error rather than a stale "already acquired" guard.
      */
    def release(): Future[Unit] = {
        if closed then return Future.failed(new IllegalStateException("driver closed"))
        if !acquired then return Future.failed(new IllegalStateException("no snapshot acquired"))
        acquired = false
        stream.send(MsgRelease, cancelToken)
    }

    /** Send `MsgDone` best-effort and mark the driver closed. If a snapshot is currently acquired,
      * sends `MsgRelease` first to reach `Idle` (the only state from which `MsgDone` is valid per
      * the protocol). Both sends are best-effort — failures are logged at debug, because the
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

    private def unexpectedMessage(state: String, msg: LocalStateQueryMessage): Throwable =
        new IllegalStateException(s"unexpected $msg in state $state")
}

object LocalStateQueryDriver {

    private val defaultLogger: scribe.Logger =
        scribe.Logger("scalus.cardano.network.n2c.localstatequery")
}
