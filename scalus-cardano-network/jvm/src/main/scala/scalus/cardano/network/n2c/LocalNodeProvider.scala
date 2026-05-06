package scalus.cardano.network.n2c

import io.bullet.borer.Cbor
import scalus.cardano.ledger.{CardanoInfo, ProtocolParams, SlotNo, Transaction, TransactionHash, Utxos}
import scalus.cardano.node.{BlockchainProvider, NodeSubmitError, SubmitError, TransactionStatus, UtxoQuery, UtxoQueryError}
import scalus.cardano.node.stream.{BackupDiagnostics, BackupDiagnosticsSnapshot}
import scalus.cardano.network.NetworkMagic
import scalus.cardano.network.infra.MiniProtocolId
import scalus.cardano.network.n2c.localtxsubmission.{LocalTxSubmissionDriver, LocalTxSubmissionRejection}
import scalus.uplc.builtin.{ByteString, Data}
import scalus.utils.Hex.toHex

import java.nio.file.Path
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.concurrent.{ExecutionContext, Future}

/** Submit-only `BlockchainProvider` over `LocalTxSubmission` against a local cardano-node.
  *
  * Owns its own [[NodeToClientConnection]] (one connection per provider instance) plus a
  * [[LocalTxSubmissionDriver]] talking on [[MiniProtocolId.LocalTxSubmission]]. Read methods
  * (`findUtxos`, `fetchLatestParams`, `currentSlot`, `getDatum`, `checkTransaction`) raise
  * [[UnsupportedOperationException]] until `LocalStateQuery` lands in M12 — pair this backup with a
  * `BackupSource.Blockfrost` if read coverage is needed before then.
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
    val cardanoInfo: CardanoInfo,
    submitEra: Int,
    connectedSinceMillis: Long
)(using val executionContext: ExecutionContext)
    extends BlockchainProvider
    with BackupDiagnostics {

    private val submitCount = new AtomicLong(0L)
    private val rejectCount = new AtomicLong(0L)
    private val lastSubmitted = new AtomicReference[Option[TransactionHash]](None)

    def submit(transaction: Transaction): Future[Either[SubmitError, TransactionHash]] = {
        val txBytes = ByteString.fromArray(Cbor.encode(transaction).toByteArray)
        submitCount.incrementAndGet()
        driver.submit(submitEra, txBytes).map {
            case Right(_) =>
                lastSubmitted.set(Some(transaction.id))
                Right(transaction.id)
            case Left(LocalTxSubmissionRejection(rejectEra, reasonBytes)) =>
                rejectCount.incrementAndGet()
                Left(
                  NodeSubmitError.ValidationError(
                    message =
                        s"local-tx-submit reject (era=$rejectEra): ${reasonBytes.bytes.toHex}",
                    errorCode = Some("LocalTxSubmissionReject")
                  )
                )
        }
    }

    def diagnostics: BackupDiagnosticsSnapshot = BackupDiagnosticsSnapshot(
      connectedSinceMillis = connectedSinceMillis,
      lastSubmittedHash = lastSubmitted.get,
      submitCount = submitCount.get,
      rejectCount = rejectCount.get
    )

    /** Tear down the driver + connection. Idempotent. */
    def close(): Future[Unit] = driver.close().flatMap(_ => conn.close())

    // ---- Read methods deferred to M12 LSQ. ----

    private def unsupportedRead(name: String): Nothing =
        throw new UnsupportedOperationException(
          s"$name is not supported by BackupSource.LocalNode — pair with BackupSource.Blockfrost " +
              "or wait for M12 (LocalStateQuery)"
        )

    override def fetchLatestParams: Future[ProtocolParams] = unsupportedRead("fetchLatestParams")
    override def findUtxos(query: UtxoQuery): Future[Either[UtxoQueryError, Utxos]] =
        unsupportedRead("findUtxos")
    override def currentSlot: Future[SlotNo] = unsupportedRead("currentSlot")
    override def getDatum(datumHash: scalus.cardano.ledger.DataHash): Future[Option[Data]] =
        unsupportedRead("getDatum")
    override def checkTransaction(txHash: TransactionHash): Future[TransactionStatus] =
        unsupportedRead("checkTransaction")
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
        NodeToClientClient.connect(socketPath, networkMagic).map { conn =>
            val driver = new LocalTxSubmissionDriver(
              conn.channel(MiniProtocolId.LocalTxSubmission),
              conn.rootToken
            )
            new LocalNodeProvider(
              conn,
              driver,
              cardanoInfo,
              submitEra,
              connectedSinceMillis = System.currentTimeMillis()
            )
        }
    }
}
