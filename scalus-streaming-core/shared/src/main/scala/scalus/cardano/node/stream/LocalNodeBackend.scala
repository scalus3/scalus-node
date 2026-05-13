package scalus.cardano.node.stream

import scalus.cardano.ledger.{CardanoInfo, DataHash, ProtocolParams, SlotNo, Transaction, TransactionHash, Utxos}
import scalus.cardano.node.{SubmitError, UtxoQuery, UtxoQueryError}
import scalus.uplc.builtin.Data

import scala.concurrent.{ExecutionContext, Future}

/** Internal facade for an N2C-style local-node client. Defined in `scalus-streaming-core` (rather
  * than `scalus-cardano-network`) so the engine + [[BaseStreamProvider]] can compose with it
  * without the network module having to depend on core's contracts — and without core needing to
  * depend on the network module (which would invert the existing arrow).
  *
  * '''Why this trait is not [[scalus.cardano.node.BlockchainProvider]].''' N2C alone cannot honour
  * the full `checkTransaction` contract — `LocalTxMonitor` sees only the mempool snapshot, so a
  * confirmed-and-evicted tx would be reported as `NotFound`. Rather than lie in the type, this
  * trait exposes the narrower [[checkInMempool]]: `Future[Boolean]` — exactly what N2C can answer.
  * The trait-conformant `BlockchainProvider.checkTransaction` is constructed one layer up, in
  * [[BaseStreamProvider#checkTransaction]], which consults the engine's `TxHashIndex` for chain
  * history and falls through to this facade only for the mempool question.
  *
  * @see
  *   [[BackupDiagnostics]] — extended here so the engine's diagnostics plumbing works uniformly
  *   across both backup kinds.
  */
trait LocalNodeBackend extends BackupDiagnostics {

    def cardanoInfo: CardanoInfo
    def executionContext: ExecutionContext

    def submit(transaction: Transaction): Future[Either[SubmitError, TransactionHash]]

    def currentSlot: Future[SlotNo]
    def fetchLatestParams: Future[ProtocolParams]
    def findUtxos(query: UtxoQuery): Future[Either[UtxoQueryError, Utxos]]
    def getDatum(datumHash: DataHash): Future[Option[Data]]

    /** Mempool-presence check. `true` iff the tx is in the local-node mempool snapshot right now;
      * `false` otherwise (never-seen, evicted, or already on chain — N2C cannot distinguish those).
      */
    def checkInMempool(txHash: TransactionHash): Future[Boolean]

    /** Tear down the underlying N2C connection + drivers. Idempotent. */
    def close(): Future[Unit]
}
