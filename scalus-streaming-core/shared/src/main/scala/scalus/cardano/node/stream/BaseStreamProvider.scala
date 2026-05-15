package scalus.cardano.node.stream

import scalus.cardano.ledger.{Block, CardanoInfo, DataHash, ProtocolParams, Transaction, TransactionHash, TransactionInput, TransactionOutput, Utxos}
import scalus.cardano.node.{BlockchainProvider, BlockchainReader, SubmitError, TransactionStatus, UtxoQuery, UtxoQueryError, UtxoSource}
import scalus.uplc.builtin.Data
import scalus.cardano.node.stream.engine.{Engine, Mailbox}

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Common implementation glue for streaming providers across adapters. All subscription plumbing
  * and snapshot-method fall-through lives here; concrete adapters only need to:
  *
  *   - provide a `MailboxSource[C]` given instance;
  *   - implement [[liftFuture]] / [[pureF]];
  *   - implement [[close]].
  *
  * Subscription flow:
  *
  *   1. Caller invokes `subscribeXxx(...)` — synchronous.
  *   2. Adapter allocates the [[Mailbox]] with the appropriate semantics (delta/latest-value) and
  *      an `onCancel` hook that asks the engine to unregister.
  *   3. Mailbox is passed to the engine's async `registerXxx`; the engine stores it and (for UTxO
  *      subs) kicks off seed-from-backup.
  *   4. Adapter wraps the mailbox into its native stream via the `MailboxSource[C]` instance and
  *      returns it.
  *
  * **Ordering invariant.** Each `subscribeXxx` enqueues the engine's `registerXxx` task
  * synchronously on the worker queue (via `Engine.submit`) before returning. Any subsequent
  * engine-bound call from the same caller thread (`applyBlock`, `submit`, `notifySubmit`, another
  * `subscribeXxx`) enqueues its own task after the registration, so the worker processes them in
  * FIFO order. Consequence: on the same thread, `subscribe(q)` immediately followed by `submit(tx)`
  * is race-free — the block's event fan-out sees the subscription.
  *
  * **Cross-thread caveat.** If one thread calls `subscribeXxx` while another is already submitting,
  * the two enqueue orders are not co-ordinated; the concurrent submit may observe the worker queue
  * before the registration lands, dropping events for that subscriber until the registration is
  * processed. Callers that need cross-thread ordering should synchronise via an external
  * happens-before (e.g. await a signal from the subscribe thread before submitting).
  */
abstract class BaseStreamProvider[F[_], C[_]](
    val engine: Engine
)(using val mailboxSource: MailboxSource[C])
    extends BlockchainStreamProviderTF[F, C] {

    /** Lightweight composition EC — callbacks do no heavy work. */
    private given composer: ExecutionContext = ExecutionContext.parasitic

    protected def liftFuture[A](fa: => Future[A]): F[A]
    protected def pureF[A](a: A): F[A]

    final def cardanoInfo: CardanoInfo = engine.cardanoInfo

    /** Snapshot of the configured backup's diagnostic counters, if the backup implements
      * [[BackupDiagnostics]]. Returns `None` when no backup is configured or the configured backup
      * doesn't expose diagnostics (e.g. `BackupSource.Blockfrost` today). Useful for deployment
      * health checks and ops dashboards.
      */
    final def backupDiagnostics: Option[BackupDiagnosticsSnapshot] =
        engine.backup
            .collect { case d: BackupDiagnostics => d.diagnostics }
            .orElse(engine.localNode.collect { case d: BackupDiagnostics => d.diagnostics })

    // ------------------------------------------------------------------
    // Stream subscriptions
    // ------------------------------------------------------------------

    final def subscribeUtxoQuery(
        query: UtxoEventQuery,
        opts: SubscriptionOptions
    ): C[UtxoEvent] = {
        val id = engine.nextSubscriptionId()
        val maxSize = opts.bufferPolicy match
            case DeltaBufferPolicy.Bounded(n) => n
            case DeltaBufferPolicy.Unbounded  => Int.MaxValue
        val mailbox = Mailbox.delta[UtxoEvent](
          maxSize,
          onCancel = () => { engine.unregisterUtxo(id); () }
        )
        engine.registerUtxoSubscription(
          id,
          query.query,
          opts.includeExistingUtxos,
          mailbox,
          opts.startFrom
        )
        mailboxSource.fromMailbox(mailbox)
    }

    final def subscribeTransactionQuery(
        query: TransactionQuery,
        opts: SubscriptionOptions
    ): C[TransactionEvent] = {
        // Not wired in M2 — return an immediately-completed stream.
        val mailbox = Mailbox.delta[TransactionEvent](Int.MaxValue)
        mailbox.close()
        mailboxSource.fromMailbox(mailbox)
    }

    final def subscribeBlockQuery(
        query: BlockQuery,
        opts: SubscriptionOptions
    ): C[BlockEvent] = {
        val mailbox = Mailbox.delta[BlockEvent](Int.MaxValue)
        mailbox.close()
        mailboxSource.fromMailbox(mailbox)
    }

    final def subscribeTip(): C[ChainTip] = {
        val id = engine.nextSubscriptionId()
        val mailbox = Mailbox.latestValue[ChainTip](
          onCancel = () => { engine.unregisterTip(id); () }
        )
        engine.registerTipSubscription(id, mailbox)
        mailboxSource.fromMailbox(mailbox)
    }

    final def subscribeProtocolParams(): C[ProtocolParams] = {
        val id = engine.nextSubscriptionId()
        val mailbox = Mailbox.latestValue[ProtocolParams](
          onCancel = () => { engine.unregisterParams(id); () }
        )
        engine.registerParamsSubscription(id, mailbox)
        mailboxSource.fromMailbox(mailbox)
    }

    final def subscribeTransactionStatus(txHash: TransactionHash): C[TransactionStatus] = {
        val id = engine.nextSubscriptionId()
        val mailbox = Mailbox.latestValue[TransactionStatus](
          onCancel = () => { engine.unregisterTxStatus(txHash, id); () }
        )
        engine.registerTxStatusSubscription(id, txHash, mailbox)
        mailboxSource.fromMailbox(mailbox)
    }

    inline def subscribeUtxo(
        inline f: (TransactionInput, TransactionOutput) => Boolean
    ): C[UtxoEvent] = ???

    inline def subscribeTransaction(inline f: Transaction => Boolean): C[TransactionEvent] = ???

    inline def subscribeBlock(inline f: Block => Boolean): C[BlockEvent] = ???

    // ------------------------------------------------------------------
    // Snapshot (one-shot) methods. Engine first; backup on fall-through.
    // ------------------------------------------------------------------

    /** Dispatch a read across the two backup slots: `engine.backup` first, then `engine.localNode`,
      * else `default`. Mutually exclusive in practice today (one per `BackupSource` variant), but
      * the helper does not assume that.
      */
    private def dispatchBackup[A](
        fromFull: BlockchainReader => Future[A],
        fromLocal: LocalNodeBackend => Future[A],
        default: => Future[A]
    ): Future[A] = engine.backup match
        case Some(bp) => fromFull(bp)
        case None =>
            engine.localNode match
                case Some(ln) => fromLocal(ln)
                case None     => default

    /** Threads the engine-side `notifySubmit` through a submit-call's result. */
    private def notifyOnSubmit(
        result: Future[Either[SubmitError, TransactionHash]]
    ): Future[Either[SubmitError, TransactionHash]] = result.flatMap {
        case r @ Right(hash) => engine.notifySubmit(hash).map(_ => r)
        case l @ Left(_)     => Future.successful(l)
    }

    final def currentSlot: F[scalus.cardano.ledger.SlotNo] = liftFuture {
        engine.currentTip match
            case Some(t) => Future.successful(t.slot)
            case None =>
                dispatchBackup(
                  _.currentSlot,
                  _.currentSlot,
                  Future.failed(Engine.NoBackupConfiguredException("currentSlot"))
                )
    }

    final def fetchLatestParams: F[ProtocolParams] = liftFuture {
        Future.successful(engine.latestParams)
    }

    final def findUtxos(query: UtxoQuery): F[Either[UtxoQueryError, Utxos]] = liftFuture {
        engine.findUtxosLocal(query).flatMap {
            case Some(utxos) => Future.successful(Right(utxos))
            case None =>
                dispatchBackup(
                  _.findUtxos(query),
                  _.findUtxos(query),
                  Future.successful(Left(UtxoQueryError.NotFound(noBackupSource(query))))
                )
        }
    }

    final def getDatum(datumHash: DataHash): F[Option[Data]] = liftFuture {
        engine.lookupDatum(datumHash).flatMap {
            case some @ Some(_) => Future.successful(some)
            case None =>
                dispatchBackup(
                  _.getDatum(datumHash),
                  _.getDatum(datumHash),
                  Future.successful(None)
                )
        }
    }

    final def checkTransaction(txHash: TransactionHash): F[TransactionStatus] = liftFuture {
        engine.txStatus(txHash).flatMap {
            case Some(status) => Future.successful(status)
            case None =>
                dispatchBackup(
                  _.checkTransaction(txHash),
                  _.checkInMempool(txHash).map {
                      if _ then TransactionStatus.Pending else TransactionStatus.NotFound
                  },
                  Future.successful(TransactionStatus.NotFound)
                )
        }
    }

    def submit(transaction: Transaction): F[Either[SubmitError, TransactionHash]] =
        liftFuture {
            engine.backup match
                case Some(bp: BlockchainProvider) => notifyOnSubmit(bp.submit(transaction))
                case _ =>
                    engine.localNode match
                        case Some(ln) => notifyOnSubmit(ln.submit(transaction))
                        case None     => Future.successful(Left(noBackupSubmitError))
        }

    /** Three-arg overload — overrides the [[scalus.cardano.node.BlockchainProviderTF]] method.
      * Equivalent to `pollForConfirmation(txHash, maxAttempts, delayMs, confirmations = 0)`:
      * preserves the M2 semantic "first sighting wins, depth = 1 implicitly."
      */
    final def pollForConfirmation(
        txHash: TransactionHash,
        maxAttempts: Int,
        delayMs: Long
    ): F[TransactionStatus] = pollForConfirmation(txHash, maxAttempts, delayMs, 0)

    /** Poll until `txHash` is at depth ≥ `confirmations` in the engine's view, or `maxAttempts`
      * exhausts. `confirmations = 0` preserves the M2 semantic; `> 0` waits until the tx is buried
      * that many blocks deep.
      *
      * Depth is engine-internal: derived from the rollback-buffer `TxHashIndex` + `currentTip`.
      * When `confirmations > 0` the backup-side `pollForConfirmation` is bypassed (it has no
      * concept of *our* depth), and the loop runs locally regardless of backup configuration.
      *
      * If the tx is confirmed at some shallower depth and the budget exhausts before the required
      * depth is reached, the returned status is `Pending` — not `Confirmed`. The caller's timeout
      * branch fires (e.g. `submitAndPoll` returns `Left`), which is the intended contract for a
      * depth-aware poll.
      *
      * `confirmations` has no default here so this overload doesn't conflict with the inherited
      * three-arg method's trait-supplied defaults (Scala disallows multiple defaulted overloads of
      * the same method name).
      */
    final def pollForConfirmation(
        txHash: TransactionHash,
        maxAttempts: Int,
        delayMs: Long,
        confirmations: Int
    ): F[TransactionStatus] = liftFuture {
        if confirmations > 0 then pollLocally(txHash, maxAttempts, delayMs, confirmations)
        else
            engine.backup match
                case Some(bp: BlockchainProvider) =>
                    engine.txStatus(txHash).flatMap {
                        case Some(status) => Future.successful(status)
                        case None         => bp.pollForConfirmation(txHash, maxAttempts, delayMs)
                    }
                case _ =>
                    // No tx-indexed backup; client-side poll via our own dispatching
                    // `checkTransaction` (which composes engine.txStatus → backup →
                    // localNode-mempool). Covers `BackupSource.LocalNode` and `NoBackup` uniformly.
                    pollLocally(txHash, maxAttempts, delayMs, 0)
    }

    private def pollLocally(
        txHash: TransactionHash,
        attemptsLeft: Int,
        delayMs: Long,
        confirmations: Int
    ): Future[TransactionStatus] = checkTransactionFuture(txHash).flatMap {
        case TransactionStatus.Confirmed if confirmations <= 0 =>
            Future.successful(TransactionStatus.Confirmed)
        case TransactionStatus.Confirmed =>
            engine.confirmationDepth(txHash).flatMap {
                case Some(d) if d >= confirmations =>
                    Future.successful(TransactionStatus.Confirmed)
                case _ if attemptsLeft <= 1 =>
                    // Confirmed at some shallower depth, but the required depth wasn't reached
                    // within budget — report `Pending` so callers' timeout branches fire.
                    Future.successful(TransactionStatus.Pending)
                case _ =>
                    sleepThenPoll(txHash, attemptsLeft, delayMs, confirmations)
            }
        case status if attemptsLeft <= 1 => Future.successful(status)
        case _ => sleepThenPoll(txHash, attemptsLeft, delayMs, confirmations)
    }

    private def sleepThenPoll(
        txHash: TransactionHash,
        attemptsLeft: Int,
        delayMs: Long,
        confirmations: Int
    ): Future[TransactionStatus] =
        BaseStreamProvider
            .scheduledDelay(delayMs)
            .flatMap(_ => pollLocally(txHash, attemptsLeft - 1, delayMs, confirmations))

    /** `checkTransaction` as a plain `Future` — needed by the local-side polling loop, which can't
      * see the effect wrapper `F`.
      */
    private def checkTransactionFuture(txHash: TransactionHash): Future[TransactionStatus] =
        engine.txStatus(txHash).flatMap {
            case Some(status) => Future.successful(status)
            case None =>
                dispatchBackup(
                  _.checkTransaction(txHash),
                  _.checkInMempool(txHash).map {
                      if _ then TransactionStatus.Pending else TransactionStatus.NotFound
                  },
                  Future.successful(TransactionStatus.NotFound)
                )
        }

    /** Three-arg overload — overrides the [[scalus.cardano.node.BlockchainProviderTF]] method.
      * Equivalent to `submitAndPoll(transaction, maxAttempts, delayMs, confirmations = 0)`.
      */
    final def submitAndPoll(
        transaction: Transaction,
        maxAttempts: Int,
        delayMs: Long
    ): F[Either[SubmitError, TransactionHash]] =
        submitAndPoll(transaction, maxAttempts, delayMs, 0)

    /** Submit + poll. When `confirmations > 0` the depth-aware local poll is used regardless of
      * backup configuration (the backup-side poll has no concept of *our* depth). Returns
      * `Right(hash)` iff the tx is confirmed at depth ≥ `confirmations` within the polling budget;
      * `Left(NetworkSubmitError.ConnectionError("not confirmed, last status: …"))` otherwise. The
      * Left carries no typed reject reason — that requires Blockfrost or N2C `LocalTxSubmission`.
      *
      * `confirmations` has no default here so this overload doesn't conflict with the inherited
      * three-arg method's trait-supplied defaults.
      */
    final def submitAndPoll(
        transaction: Transaction,
        maxAttempts: Int,
        delayMs: Long,
        confirmations: Int
    ): F[Either[SubmitError, TransactionHash]] = liftFuture {
        def afterSubmit(
            submitFut: Future[Either[SubmitError, TransactionHash]],
            serverSidePoll: Option[TransactionHash => Future[TransactionStatus]]
        ): Future[Either[SubmitError, TransactionHash]] = submitFut.flatMap {
            case l @ Left(_) => Future.successful(l)
            case Right(hash) =>
                engine.notifySubmit(hash).flatMap { _ =>
                    val poll: TransactionHash => Future[TransactionStatus] =
                        if confirmations > 0 then
                            h => pollLocally(h, maxAttempts, delayMs, confirmations)
                        else
                            serverSidePoll
                                .getOrElse(h => pollLocally(h, maxAttempts, delayMs, 0))
                    poll(hash).map {
                        case TransactionStatus.Confirmed => Right(hash)
                        case other =>
                            Left(
                              scalus.cardano.node.NetworkSubmitError.ConnectionError(
                                s"tx $hash not confirmed, last status: $other"
                              )
                            )
                    }
                }
        }
        engine.backup match
            case Some(bp: BlockchainProvider) =>
                afterSubmit(
                  bp.submit(transaction),
                  Some(bp.pollForConfirmation(_, maxAttempts, delayMs))
                )
            case _ =>
                engine.localNode match
                    case Some(ln) => afterSubmit(ln.submit(transaction), None)
                    case None     => Future.successful(Left(noBackupSubmitError))
    }

    private def noBackupSource(q: UtxoQuery): UtxoSource =
        q match
            case UtxoQuery.Simple(source, _, _, _, _) => source
            case UtxoQuery.Or(left, _, _, _, _)       => noBackupSource(left)

    private def noBackupSubmitError: SubmitError =
        scalus.cardano.node.NetworkSubmitError.ConnectionError("no backup source configured")
}

object BaseStreamProvider {

    /** Process-wide daemon scheduler used by [[pollLocally]] to release the EC thread during
      * inter-attempt delays. Single thread is enough — it does only the wake-up; the woken
      * continuation runs on the caller's EC via `Promise.future.flatMap`. Daemon so JVM exit isn't
      * held up.
      */
    private lazy val pollScheduler: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor { r =>
            val t = new Thread(r, "scalus-stream-poll-scheduler")
            t.setDaemon(true)
            t
        }

    /** Non-blocking delay — returns a `Future[Unit]` that completes after `delayMs` without holding
      * an EC thread. Replaces `Thread.sleep` inside the polling loop, which previously blocked one
      * EC thread per long poll.
      */
    private def scheduledDelay(delayMs: Long): Future[Unit] = {
        val p = Promise[Unit]()
        val _ = pollScheduler.schedule(
          new Runnable { def run(): Unit = { val _ = p.success(()) } },
          delayMs,
          TimeUnit.MILLISECONDS
        )
        p.future
    }
}
