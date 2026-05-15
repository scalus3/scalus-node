package scalus.cardano.network.txsubmission

import cps.*
import cps.monads.FutureAsyncMonad
import io.bullet.borer.Cbor
import scalus.cardano.infra.{CancelToken, CancelledException}
import scalus.cardano.ledger.{Transaction, TransactionHash}
import scalus.cardano.network.infra.{CborMessageStream, MiniProtocolBytes, MiniProtocolId}
import scalus.cardano.network.txsubmission.TxSubmission2Message.*
import scalus.cardano.node.{NetworkSubmitError, SubmitError}
import scalus.cardano.node.stream.N2nTxSubmissionBackend
import scalus.uplc.builtin.ByteString

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Producer-side driver for the TxSubmission2 mini-protocol (id 4).
  *
  * Role inversion vs. N2C `LocalTxSubmission`: the peer (cardano-node) is the *consumer*; we are
  * the *producer / responder*. The peer drives the conversation:
  *
  * {{{
  *   client → server:   MsgInit
  *   server → client:   MsgRequestTxIds(blocking, numAck, numReq)
  *   client → server:     MsgReplyTxIds([(txid, size), …])
  *   server → client:   MsgRequestTxs([txid, …])
  *   client → server:     MsgReplyTxs([txBody, …])
  *   … repeat …
  *   either side:       MsgDone (on shutdown)
  * }}}
  *
  * The driver owns an in-memory FIFO of submitted transactions; the head of the queue is what the
  * peer pulls. `numAck` in subsequent `MsgRequestTxIds` consumes from the front.
  *
  * **No acceptance signal.** TxSubmission2 has no `MsgRejectTx`. [[submit]] returns
  * `Future.successful(transaction.id)` as soon as the tx is enqueued; what happens after (the peer
  * pulling, the peer's mempool admitting, the tx reaching a block) is observable only via the
  * engine's chain-sync side (`subscribeTransactionStatus` + the depth-aware
  * `submitAndPoll(confirmations = K)` pair on `BaseStreamProvider`).
  *
  * **Single-peer.** The driver runs on one `NodeToNodeConnection`'s `TxSubmission` channel.
  * Multi-peer mempool propagation is out of scope for M8.
  *
  * **Lifecycle.** [[run]] returns a `Future[Unit]` that completes when the loop exits — on
  * `MsgDone` from the peer, on stream EOF, on `cancelToken` cancellation, or on [[close]].
  * Best-effort `MsgDone` is sent on the way out.
  */
final class TxSubmission2Driver(
    handle: MiniProtocolBytes,
    cancelToken: CancelToken,
    submitEra: Int = TxSubmission2Driver.ConwayEra,
    logger: scribe.Logger = TxSubmission2Driver.defaultLogger
)(using ec: ExecutionContext)
    extends N2nTxSubmissionBackend {

    import TxSubmission2Driver.QueuedTx

    private val stream =
        new CborMessageStream[TxSubmission2Message](MiniProtocolId.TxSubmission, handle)

    private val lock = new AnyRef
    private val queue: mutable.ArrayDeque[QueuedTx] = mutable.ArrayDeque.empty
    private val byHash: mutable.Map[TransactionHash, QueuedTx] = mutable.Map.empty
    private var newSubmitWaiters: mutable.ArrayBuffer[Promise[Unit]] = mutable.ArrayBuffer.empty
    @volatile private var closed: Boolean = false

    /** [[N2nTxSubmissionBackend.submit]]: enqueue `transaction` for the peer to pull. Returns
      * `Right(transaction.id)` immediately on enqueue; `Left(ConnectionError)` only if the driver
      * has been closed or the underlying connection is gone. No typed reject reason — the protocol
      * doesn't carry one.
      */
    def submit(transaction: Transaction): Future[Either[SubmitError, TransactionHash]] =
        submitWire(
          transaction.id,
          ByteString.unsafeFromArray(Cbor.encode(transaction).toByteArray)
        ).map(Right(_): Either[SubmitError, TransactionHash])
            .recover { case t: IllegalStateException =>
                Left(NetworkSubmitError.ConnectionError(t.getMessage))
            }

    /** Wire-level submit — same effect as [[submit]] but takes the hash and pre-encoded CBOR
      * directly. Used by tests that want to exercise the protocol without constructing a full
      * [[Transaction]]; not exposed publicly.
      */
    private[txsubmission] def submitWire(
        hash: TransactionHash,
        txCbor: ByteString
    ): Future[TransactionHash] = lock.synchronized {
        if closed then Future.failed(new IllegalStateException("driver closed"))
        else {
            val q = QueuedTx(
              hash = hash,
              wireId = TxId(submitEra, ByteString.unsafeFromArray(hash.bytes)),
              wireBody = TxBody(submitEra, txCbor),
              size = txCbor.size.toLong
            )
            queue += q
            byHash(hash) = q
            wakeAllWaitersLocked()
            Future.successful(hash)
        }
    }

    /** Snapshot of currently-queued tx hashes (in offer order). Useful for diagnostics and tests.
      */
    def pendingSubmissions: Seq[TransactionHash] =
        lock.synchronized(queue.iterator.map(_.hash).toSeq)

    /** [[N2nTxSubmissionBackend.close]]: mark the driver closed and unblock any handlers waiting on
      * a blocking `MsgRequestTxIds`. The mini-protocol channel is torn down separately when the
      * `NodeToNodeConnection`'s root cancels — at which point [[run]]'s `done` future completes and
      * the best-effort `MsgDone` is emitted on the way out. Idempotent.
      */
    def close(): Future[Unit] = {
        lock.synchronized {
            if !closed then {
                closed = true
                wakeAllWaitersLocked()
            }
        }
        Future.unit
    }

    /** Run the main loop. Returns a Future that completes when the loop exits. The caller (the
      * provider's `connectN2N`) typically spawns this once at connection setup and awaits it as
      * part of teardown.
      */
    def run(): Future[Unit] = {
        val loop = async[Future] {
            await(stream.send(MsgInit, cancelToken))
            var running = true
            while running && !closed && !cancelToken.isCancelled do {
                val received = await(stream.receive(cancelToken))
                received match {
                    case Some(MsgRequestTxIds(blocking, ack, req)) =>
                        await(handleRequestTxIds(blocking, ack, req))
                    case Some(MsgRequestTxs(ids)) =>
                        await(handleRequestTxs(ids))
                    case Some(MsgDone) =>
                        logger.info("peer sent MsgDone; stopping")
                        running = false
                    case Some(MsgInit) | Some(_: MsgReplyTxIds) | Some(_: MsgReplyTxs) =>
                        // Wire violation — we're the producer; peer must never send these.
                        throw new IllegalStateException(
                          s"peer sent producer-side message: $received"
                        )
                    case None =>
                        logger.info("TxSubmission2 stream EOF; stopping")
                        running = false
                }
            }
        }
        loop.transformWith { result =>
            // Best-effort MsgDone on the way out. Failure here is a debug log, not propagated.
            stream
                .send(MsgDone, CancelToken.never)
                .recover { case t =>
                    logger.debug(s"MsgDone best-effort send failed: $t")
                    ()
                }
                .flatMap(_ => Future.fromTry(result.recover { case _: CancelledException => () }))
        }
    }

    // ----------------------------------------------------------------------------------------

    private def handleRequestTxIds(
        blocking: Boolean,
        numAck: Int,
        numReq: Int
    ): Future[Unit] = {
        lock.synchronized {
            var n = numAck
            while n > 0 && queue.nonEmpty do {
                val removed = queue.removeHead()
                byHash.remove(removed.hash)
                n -= 1
            }
        }
        val awaitFut: Future[Unit] =
            if !blocking then Future.unit
            else awaitNonEmptyOrShutdown()
        awaitFut.flatMap { _ =>
            val ids = lock.synchronized {
                queue.iterator.take(numReq).map(q => q.wireId -> q.size).toSeq
            }
            stream.send(MsgReplyTxIds(ids), cancelToken)
        }
    }

    private def handleRequestTxs(ids: Seq[TxId]): Future[Unit] = {
        val bodies: Seq[TxBody] = lock.synchronized {
            ids.map { txid =>
                val hash = TransactionHash.fromArray(txid.hashBytes.bytes)
                byHash.get(hash) match {
                    case Some(q) => q.wireBody
                    case None =>
                        throw new IllegalStateException(
                          s"peer requested tx body for unknown id $hash"
                        )
                }
            }
        }
        stream.send(MsgReplyTxs(bodies), cancelToken)
    }

    /** Resolve when the queue becomes non-empty, the driver is closed, or the cancel token fires.
      * The blocking-`MsgRequestTxIds` branch loops on this until the loop's exit condition fires.
      */
    private def awaitNonEmptyOrShutdown(): Future[Unit] = {
        val p = Promise[Unit]()
        lock.synchronized {
            if queue.nonEmpty || closed || cancelToken.isCancelled then
                val _ = p.trySuccess(())
            else newSubmitWaiters += p
        }
        p.future
    }

    private def wakeAllWaitersLocked(): Unit = {
        val waiters = newSubmitWaiters
        newSubmitWaiters = mutable.ArrayBuffer.empty
        waiters.foreach(p => { val _ = p.trySuccess(()) })
    }
}

object TxSubmission2Driver {

    /** HardForkCombinator era index for Conway (current as of 2026). Bump when the next era ships;
      * the driver's `submitEra` constructor param lets callers override per-deployment.
      */
    val ConwayEra: Int = 6

    private val defaultLogger: scribe.Logger =
        scribe.Logger("scalus.cardano.network.txsubmission")

    /** Internal queue entry — pairs an engine-facing [[TransactionHash]] with its wire-level
      * representations (id + body) so the responder loop can look up either without re-encoding.
      */
    private[txsubmission] final case class QueuedTx(
        hash: TransactionHash,
        wireId: TxId,
        wireBody: TxBody,
        size: Long
    )
}
