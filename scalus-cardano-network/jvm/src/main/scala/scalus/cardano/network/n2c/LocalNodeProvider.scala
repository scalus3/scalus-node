package scalus.cardano.network.n2c

import io.bullet.borer.Cbor
import scalus.cardano.ledger.{CardanoInfo, ConwayProtocolParams, ProtocolParams, SlotNo, Transaction, TransactionHash, Utxos}
import scalus.cardano.node.{BlockchainProvider, NodeSubmitError, SubmitError, TransactionStatus, UtxoQuery, UtxoQueryError, UtxoSource}
import scalus.cardano.node.stream.{BackupDiagnostics, BackupDiagnosticsSnapshot}
import scalus.cardano.network.NetworkMagic
import scalus.cardano.network.chainsync.Point
import scalus.cardano.network.infra.MiniProtocolId
import scalus.cardano.network.n2c.localstatequery.{LocalStateQueryDriver, LocalStateQueryMessage, LsqQuery}
import scalus.cardano.network.n2c.localtxmonitor.LocalTxMonitorDriver
import scalus.cardano.network.n2c.localtxsubmission.{LocalTxSubmissionDriver, LocalTxSubmissionRejection}
import scalus.uplc.builtin.{ByteString, Data}
import scalus.utils.Hex.toHex

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}

/** `BlockchainProvider` against a local cardano-node over N2C.
  *
  * Owns its own [[NodeToClientConnection]] plus three mini-protocol drivers:
  *   - [[LocalTxSubmissionDriver]] on [[MiniProtocolId.LocalTxSubmission]] for `submit`
  *   - [[LocalStateQueryDriver]] on [[MiniProtocolId.LocalStateQuery]] for `currentSlot` /
  *     `fetchLatestParams` / `findUtxos`
  *   - [[LocalTxMonitorDriver]] on [[MiniProtocolId.LocalTxMonitor]] for `checkTransaction`
  *
  * Reads backed today: `currentSlot`, `fetchLatestParams` (Conway-only), `findUtxos` (trivial
  * single-source `FromAddress` / `FromInputs` queries — richer shapes return
  * [[UtxoQueryError.NotSupported]]), `checkTransaction` (mempool snapshot → `Pending` if present,
  * `NotFound` otherwise; on-chain confirmation requires upstream chain history that this provider
  * does not see). `getDatum` returns `None` because LSQ has no datum-by-hash query.
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
final class LocalNodeProvider private (
    conn: NodeToClientConnection,
    driver: LocalTxSubmissionDriver,
    lsqDriver: LocalStateQueryDriver,
    txMonitorDriver: LocalTxMonitorDriver,
    val cardanoInfo: CardanoInfo,
    submitEra: Int,
    connectedSinceMillis: Long
)(using val executionContext: ExecutionContext)
    extends BlockchainProvider
    with BackupDiagnostics {

    // Single AtomicReference holds the snapshot; CAS-update on submit/reject so the
    // BackupDiagnostics contract's "coherent point-in-time view" is real, not approximate.
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
        driver.submit(submitEra, txBytes).map {
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

    def diagnostics: BackupDiagnosticsSnapshot = diagState.get

    /** Tear down all drivers + connection. Idempotent. */
    def close(): Future[Unit] =
        txMonitorDriver
            .close()
            .flatMap(_ => lsqDriver.close())
            .flatMap(_ => driver.close())
            .flatMap(_ => conn.close())

    // -------- LSQ-backed reads --------

    override def currentSlot: Future[SlotNo] = withLsqSnapshot {
        lsqDriver.query(LsqQuery.GetChainPoint).map {
            case Point.Origin              => 0L
            case Point.BlockPoint(slot, _) => slot
        }
    }

    override def fetchLatestParams: Future[ProtocolParams] = withLsqSnapshot {
        lsqDriver.query(
          LsqQuery.GetCurrentPParams(
            era = submitEra,
            decoder = bytes => ConwayProtocolParams.fromCbor(bytes).toProtocolParams
          )
        )
    }

    /** Only single-source `FromAddress(addr)` and `FromInputs(inputs)` map cleanly onto LSQ; the
      * filter / limit / offset / minTotal facets of [[UtxoQuery.Simple]] and the `Or`/`And` source
      * combinators have no LSQ representation. Callers wanting richer queries should pair with a
      * `BackupSource.Blockfrost`; here we surface them as [[UtxoQueryError.NotSupported]] rather
      * than fetching everything and filtering client-side.
      */
    override def findUtxos(query: UtxoQuery): Future[Either[UtxoQueryError, Utxos]] = query match {
        case UtxoQuery.Simple(UtxoSource.FromAddress(addr), None, None, None, None) =>
            runUtxoLsq(LsqQuery.GetUTxOByAddress(era = submitEra, addresses = Set(addr)))
        case UtxoQuery.Simple(UtxoSource.FromInputs(inputs), None, None, None, None) =>
            runUtxoLsq(LsqQuery.GetUTxOByTxIn(era = submitEra, inputs = inputs))
        case other =>
            Future.successful(
              Left(
                UtxoQueryError.NotSupported(
                  query = other,
                  reason = "LocalNodeProvider supports only Simple(FromAddress) / " +
                      "Simple(FromInputs) without filter/limit/offset/minTotal; " +
                      "pair with BackupSource.Blockfrost for richer queries"
                )
              )
            )
    }

    private def runUtxoLsq(q: LsqQuery[Utxos]): Future[Either[UtxoQueryError, Utxos]] =
        withLsqSnapshot(lsqDriver.query(q).map(Right(_)))

    /** Async mutex around `acquire → body → release`. Multiple concurrent provider calls (e.g.
      * parallel `currentSlot` invocations) would otherwise race the LSQ driver's single-in-flight
      * contract. The previous gate is awaited before this op runs; the new gate is completed
      * regardless of op outcome.
      */
    private val lsqGate = new AtomicReference[Future[Unit]](Future.unit)

    private def withLsqLock[A](op: => Future[A]): Future[A] = {
        val nextGate = Promise[Unit]()
        val prev = lsqGate.getAndSet(nextGate.future)
        prev.transformWith(_ => op).andThen { case _ => nextGate.success(()) }
    }

    private def withLsqSnapshot[A](body: => Future[A]): Future[A] = withLsqLock {
        lsqDriver.acquire(LocalStateQueryMessage.AcquireTarget.VolatileTip).flatMap {
            case Right(()) =>
                body.transformWith { result =>
                    lsqDriver
                        .release()
                        .recover { case _ => () }
                        .flatMap(_ => Future.fromTry(result))
                }
            case Left(failure) =>
                Future.failed(new RuntimeException(s"LSQ acquire failed: $failure"))
        }
    }

    // -------- LocalTxMonitor-backed reads --------

    /** Mempool-presence answer: `Pending` if the tx is in the held mempool snapshot, `NotFound`
      * otherwise. We do not observe chain history here; a `Confirmed` answer would require the
      * upstream engine to consult its rollback buffer or chainstore. Callers that need `Confirmed`
      * should layer on top.
      */
    override def checkTransaction(txHash: TransactionHash): Future[TransactionStatus] =
        withTxMonitorSnapshot {
            txMonitorDriver.hasTx(submitEra, txHash).map { present =>
                if present then TransactionStatus.Pending else TransactionStatus.NotFound
            }
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

    override def getDatum(datumHash: scalus.cardano.ledger.DataHash): Future[Option[Data]] =
        Future.successful(None)
}

object LocalNodeProvider {

    /** Connect a fresh N2C connection at `socketPath` and return a submit-only provider. The caller
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
    )(using ExecutionContext): Future[LocalNodeProvider] = {
        // `query=false` despite this provider being LSQ-backed: in N2C version-data the `query`
        // flag does NOT enable LSQ — LSQ runs on any negotiated connection. It signals "this is a
        // version-query probe" and makes the peer reply with `MsgQueryReply` (their full version
        // table) instead of negotiating a connection. Setting `query=true` here breaks the
        // handshake against a real cardano-node. The M12.P1.a comment about "permit LSQ queries"
        // was incorrect — corrected after the yaci-devkit IT surfaced the issue.
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
            new LocalNodeProvider(
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
