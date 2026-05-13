package scalus.cardano.network.n2c

import io.bullet.borer.Cbor
import scalus.cardano.ledger.{CardanoInfo, ConwayProtocolParams, ProtocolParams, SlotNo, Transaction, TransactionHash, Utxos}
import scalus.cardano.node.{NodeSubmitError, SubmitError, UtxoQuery, UtxoQueryError, UtxoSource}
import scalus.cardano.node.stream.{BackupDiagnostics, BackupDiagnosticsSnapshot, LocalNodeBackend}
import scalus.cardano.network.NetworkMagic
import scalus.cardano.network.chainsync.Point
import scalus.cardano.network.infra.MiniProtocolId
import scalus.cardano.network.n2c.localstatequery.{LocalStateQueryDriver, LocalStateQueryMessage, LsqError, LsqQuery}
import scalus.cardano.network.n2c.localtxmonitor.LocalTxMonitorDriver
import scalus.cardano.network.n2c.localtxsubmission.{LocalTxSubmissionDriver, LocalTxSubmissionRejection}
import scalus.uplc.builtin.{ByteString, Data}
import scalus.utils.Hex.toHex

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Raw N2C facade for a local cardano-node socket. Owns one [[NodeToClientConnection]] plus three
  * mini-protocol drivers:
  *   - [[LocalTxSubmissionDriver]] on [[MiniProtocolId.LocalTxSubmission]] for `submit`
  *   - [[LocalStateQueryDriver]] on [[MiniProtocolId.LocalStateQuery]] for `currentSlot` /
  *     `fetchLatestParams` / `findUtxos`
  *   - [[LocalTxMonitorDriver]] on [[MiniProtocolId.LocalTxMonitor]] for [[checkInMempool]]
  *
  * Implements [[LocalNodeBackend]] — the narrow capability trait defined in
  * `scalus-streaming-core`. The trait deliberately exposes [[checkInMempool]] (`Future[Boolean]`)
  * instead of a full `checkTransaction`, because N2C alone cannot answer `Confirmed`. The streaming
  * engine consults its own `TxHashIndex` ahead of falling through to this facade for the mempool
  * half, so [[scalus.cardano.node.stream.BaseStreamProvider#checkTransaction]] returns a
  * contract-complete `Confirmed`/`Pending`/`NotFound`.
  *
  * Reads backed today: `currentSlot`, `fetchLatestParams` (Conway-only), `findUtxos` (single-source
  * `FromAddress(_)` / `FromInputs(_)` only — richer shapes return [[UtxoQueryError.NotSupported]]),
  * `checkInMempool` (mempool snapshot → `true` iff tx present). `getDatum` returns `None` because
  * LSQ has no datum-by-hash query.
  *
  * The N2C handshake negotiates `query = false` — `query` in N2C version-data is a version-query
  * probe flag, not an LSQ enablement. LSQ runs on any negotiated connection (the LSQ mini-protocol
  * id 7 is unconditionally available once V16+ is accepted).
  *
  * Connection-sharing with `ChainSyncSource.N2C` (when both point at the same socket) is a planned
  * optimisation; today each component opens its own connection.
  *
  * Submit-side error mapping: `MsgRejectTx(era, reason)` is wrapped into
  * [[NodeSubmitError.ValidationError]] with the reason hex-encoded into `message` and
  * `errorCode = Some("LocalTxSubmissionReject")`. Typed decomposition into a structured
  * `ApplyTxError` requires either an upstream `SubmitError` extension or a downstream
  * `RejectReason.Cbor(bytes, era)` decoder; both are deferred to a follow-up.
  */
final class LocalNodeAccess private (
    conn: NodeToClientConnection,
    submitDriver: LocalTxSubmissionDriver,
    lsqDriver: LocalStateQueryDriver,
    txMonitorDriver: LocalTxMonitorDriver,
    val cardanoInfo: CardanoInfo,
    submitEra: Int,
    connectedSinceMillis: Long
)(using val executionContext: ExecutionContext)
    extends LocalNodeBackend
    with BackupDiagnostics {

    private val diagState = new AtomicReference[BackupDiagnosticsSnapshot](
      BackupDiagnosticsSnapshot(
        connectedSinceMillis = connectedSinceMillis,
        lastSubmittedHash = None,
        submitCount = 0L,
        rejectCount = 0L
      )
    )

    private def updateDiag(f: BackupDiagnosticsSnapshot => BackupDiagnosticsSnapshot): Unit = {
        @scala.annotation.tailrec
        def loop(): Unit = {
            val cur = diagState.get
            val next = f(cur)
            if !diagState.compareAndSet(cur, next) then loop()
        }
        loop()
    }

    def submit(transaction: Transaction): Future[Either[SubmitError, TransactionHash]] = {
        val txBytes = ByteString.fromArray(Cbor.encode(transaction).toByteArray)
        updateDiag(s => s.copy(submitCount = s.submitCount + 1L))
        submitDriver.submit(submitEra, txBytes).map {
            case Right(_) =>
                updateDiag(s => s.copy(lastSubmittedHash = Some(transaction.id)))
                Right(transaction.id)
            case Left(LocalTxSubmissionRejection(rejectEra, reasonBytes)) =>
                updateDiag(s => s.copy(rejectCount = s.rejectCount + 1L))
                Left(
                  NodeSubmitError.ValidationError(
                    message =
                        s"local-tx-submit reject (era=$rejectEra): ${reasonBytes.bytes.toHex}",
                    errorCode = Some("LocalTxSubmissionReject")
                  )
                )
        }
    }

    /** Point-in-time submit-side counters. The composite that exposes this facade as
      * `BackupDiagnostics` simply forwards to this method.
      */
    def diagnostics: BackupDiagnosticsSnapshot = diagState.get

    /** Tear down all drivers + connection. Idempotent. */
    def close(): Future[Unit] =
        txMonitorDriver
            .close()
            .flatMap(_ => lsqDriver.close())
            .flatMap(_ => submitDriver.close())
            .flatMap(_ => conn.close())

    // -------- LSQ-backed reads --------

    def currentSlot: Future[SlotNo] = runLsq(LsqQuery.GetChainPoint).map {
        case Point.Origin              => 0L
        case Point.BlockPoint(slot, _) => slot
    }

    def fetchLatestParams: Future[ProtocolParams] = runLsq(
      LsqQuery.GetCurrentPParams(
        era = submitEra,
        decoder = bytes => ConwayProtocolParams.fromCbor(bytes).toProtocolParams
      )
    )

    /** Only single-source `FromAddress(addr)` and `FromInputs(inputs)` map cleanly onto LSQ; the
      * filter / limit / offset / minTotal facets of [[UtxoQuery.Simple]] and the `Or`/`And` source
      * combinators have no LSQ representation. Callers wanting richer queries should pair with a
      * `BackupSource.Blockfrost`; here we surface them as [[UtxoQueryError.NotSupported]] rather
      * than fetching everything and filtering client-side.
      *
      * `LsqError` from the underlying LSQ driver always maps to `UtxoQueryError.NotSupported`
      * (`reason` carries the typed `LsqError` rendering) — the stream stack treats `NotSupported`
      * as the "this backup can't answer, try another" signal.
      */
    def findUtxos(query: UtxoQuery): Future[Either[UtxoQueryError, Utxos]] = {
        val lsq: Option[LsqQuery[Utxos]] = query match {
            case UtxoQuery.Simple(UtxoSource.FromAddress(addr), None, None, None, None) =>
                Some(LsqQuery.GetUTxOByAddress(era = submitEra, addresses = Set(addr)))
            case UtxoQuery.Simple(UtxoSource.FromInputs(inputs), None, None, None, None) =>
                Some(LsqQuery.GetUTxOByTxIn(era = submitEra, inputs = inputs))
            case _ => None
        }
        lsq match {
            case Some(q) =>
                withLsqSnapshot(lsqDriver.query(q)).map {
                    case Right(utxos) => Right(utxos)
                    case Left(err) =>
                        Left(UtxoQueryError.NotSupported(query, reason = err.getMessage))
                }
            case None =>
                Future.successful(
                  Left(
                    UtxoQueryError.NotSupported(
                      query = query,
                      reason = "LocalNodeAccess supports only Simple(FromAddress) / " +
                          "Simple(FromInputs) without filter/limit/offset/minTotal; " +
                          "pair with BackupSource.Blockfrost for richer queries"
                    )
                  )
                )
        }
    }

    /** Acquire-query-release a single typed `LsqQuery`. Surfaces `LsqError` as `Future.failed`
      * because every `LsqError` case is itself a `RuntimeException` — methods using this helper
      * don't have a typed-error channel in `BlockchainProvider`.
      */
    private def runLsq[A](q: LsqQuery[A]): Future[A] =
        withLsqSnapshot(lsqDriver.query(q)).flatMap(_.fold(Future.failed, Future.successful))

    private val lsqGate = new AtomicReference[Future[Unit]](Future.unit)

    private def withLsqLock[A](op: => Future[A]): Future[A] = {
        val nextGate = Promise[Unit]()
        val prev = lsqGate.getAndSet(nextGate.future)
        prev.transformWith(_ => op).andThen { case _ => nextGate.success(()) }
    }

    /** Acquire a snapshot, run `body`, release. `body` may itself return a typed `Left(LsqError)`
      * (e.g. an era-mismatch on the inner query) — both acquire-time and query-time errors flow
      * through the same `Either` so callers see one error channel.
      */
    private def withLsqSnapshot[A](
        body: => Future[Either[LsqError, A]]
    ): Future[Either[LsqError, A]] = withLsqLock {
        lsqDriver.acquire(LocalStateQueryMessage.AcquireTarget.VolatileTip).flatMap {
            case Right(()) =>
                body.transformWith { result =>
                    lsqDriver.release().recover { case _ => () }.transform(_ => result)
                }
            case Left(failure) =>
                Future.successful(Left(LsqError.AcquireRejected(failure)))
        }
    }

    // -------- LocalTxMonitor-backed mempool view --------

    /** Implements [[LocalNodeBackend.checkInMempool]]. Returns `true` iff `txHash` is in the local
      * mempool snapshot right now; `false` otherwise. N2C cannot distinguish "never seen" from
      * "already on chain" — the streaming engine's `TxHashIndex` handles the on-chain side.
      */
    def checkInMempool(txHash: TransactionHash): Future[Boolean] =
        withTxMonitorSnapshot {
            txMonitorDriver.hasTx(submitEra, txHash)
        }

    private val txMonitorGate = new AtomicReference[Future[Unit]](Future.unit)

    private def withTxMonitorSnapshot[A](body: => Future[A]): Future[A] = {
        val nextGate = Promise[Unit]()
        val prev = txMonitorGate.getAndSet(nextGate.future)
        prev
            .transformWith(_ => txMonitorDriver.acquire())
            .flatMap { _ =>
                body.transformWith { result =>
                    txMonitorDriver
                        .release()
                        .recover { case _ => () }
                        .flatMap(_ => Future.fromTry(result))
                }
            }
            .andThen { case _ => nextGate.success(()) }
    }

    def getDatum(datumHash: scalus.cardano.ledger.DataHash): Future[Option[Data]] =
        Future.successful(None)
}

object LocalNodeAccess {

    /** Connect a fresh N2C connection at `socketPath` and return a [[LocalNodeAccess]]. The caller
      * owns lifecycle: invoke `close()` when done.
      *
      * @param submitEra
      *   HardForkCombinator era index used in the wire envelope of `MsgSubmitTx`. Defaults to
      *   Conway (6); switch to a higher index after the next hard fork.
      */
    def connect(
        socketPath: Path,
        networkMagic: NetworkMagic,
        cardanoInfo: CardanoInfo,
        submitEra: Int = 6
    )(using ExecutionContext): Future[LocalNodeAccess] = {
        // `query=false` despite this facade being LSQ-backed: in N2C version-data the `query`
        // flag does NOT enable LSQ — LSQ runs on any negotiated connection. It signals "this is a
        // version-query probe" and makes the peer reply with `MsgQueryReply` (their full version
        // table) instead of negotiating a connection. Setting `query=true` here breaks the
        // handshake against a real cardano-node.
        val config = ClientConfig.default
        NodeToClientClient.connect(socketPath, networkMagic, config).map { conn =>
            val submitDriver = new LocalTxSubmissionDriver(
              conn.channel(MiniProtocolId.LocalTxSubmission),
              conn.rootToken
            )
            val lsqDriver = new LocalStateQueryDriver(
              conn.channel(MiniProtocolId.LocalStateQuery),
              conn.rootToken
            )
            val txMonitorDriver = new LocalTxMonitorDriver(
              conn.channel(MiniProtocolId.LocalTxMonitor),
              conn.rootToken
            )
            new LocalNodeAccess(
              conn,
              submitDriver,
              lsqDriver,
              txMonitorDriver,
              cardanoInfo,
              submitEra,
              connectedSinceMillis = System.currentTimeMillis()
            )
        }
    }
}
