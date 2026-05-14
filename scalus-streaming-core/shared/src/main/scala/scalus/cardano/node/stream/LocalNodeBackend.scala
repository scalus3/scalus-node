package scalus.cardano.node.stream

import scalus.cardano.ledger.{CardanoInfo, DataHash, ProtocolParams, SlotNo, Transaction, TransactionHash, Utxos}
import scalus.cardano.node.{SubmitError, UtxoQuery, UtxoQueryError}
import scalus.uplc.builtin.Data

import scala.concurrent.{ExecutionContext, Future}

/** Capability surface of an N2C-style local-node client. Narrower than
  * [[scalus.cardano.node.BlockchainProvider]]: exposes [[checkInMempool]] `Future[Boolean]` instead
  * of `checkTransaction`, because N2C alone cannot distinguish "confirmed on chain" from "never
  * seen". The engine composes this with its own `TxHashIndex` to honour the full contract.
  *
  * Implementations MAY additionally mix in [[BackupDiagnostics]] if they track submit counters;
  * [[BaseStreamProvider#backupDiagnostics]] discovers it via runtime type-check, matching the
  * existing pattern for [[scalus.cardano.node.BlockchainProvider]].
  */
trait LocalNodeBackend {

    def cardanoInfo: CardanoInfo
    def executionContext: ExecutionContext

    def submit(transaction: Transaction): Future[Either[SubmitError, TransactionHash]]

    def currentSlot: Future[SlotNo]
    def fetchLatestParams: Future[ProtocolParams]
    def findUtxos(query: UtxoQuery): Future[Either[UtxoQueryError, Utxos]]
    def getDatum(datumHash: DataHash): Future[Option[Data]]

    /** `true` iff the tx is in the local-node mempool snapshot right now. */
    def checkInMempool(txHash: TransactionHash): Future[Boolean]

    /** Returns the subset of `txHashes` currently present in the local-node mempool. Default impl
      * issues one `checkInMempool` per hash; backends that can amortise the snapshot lifecycle
      * (e.g. one LTM acquire + N `hasTx` + release) should override.
      *
      * Used by the streaming engine on each block arrival to fan `Pending` events out to
      * `subscribeTransactionStatus` consumers for txs submitted by *other* clients — the engine
      * already tracks our own submits via `notifySubmit`.
      */
    def checkInMempoolBatch(
        txHashes: Set[TransactionHash]
    ): Future[Set[TransactionHash]] = {
        given ExecutionContext = executionContext
        if txHashes.isEmpty then Future.successful(Set.empty)
        else
            Future
                .traverse(txHashes.toSeq) { h =>
                    checkInMempool(h).map(present => if present then Some(h) else None)
                }
                .map(_.flatten.toSet)
    }

    /** Tear down the underlying N2C connection + drivers. Idempotent. */
    def close(): Future[Unit]
}

/** Resolved engine-backup slots produced from a [[BackupSource]]. Today exactly one is `Some`
  * (`BackupSource` variants are mutually exclusive); the case class shape leaves room for a future
  * "mixed" config without forcing every call site to unpack a tuple.
  */
final case class BackupSlots(
    full: Option[scalus.cardano.node.BlockchainProvider],
    localNode: Option[LocalNodeBackend]
)

object BackupSlots {
    val empty: BackupSlots = BackupSlots(None, None)
    def full(p: scalus.cardano.node.BlockchainProvider): BackupSlots = BackupSlots(Some(p), None)
    def localNode(ln: LocalNodeBackend): BackupSlots = BackupSlots(None, Some(ln))
}
