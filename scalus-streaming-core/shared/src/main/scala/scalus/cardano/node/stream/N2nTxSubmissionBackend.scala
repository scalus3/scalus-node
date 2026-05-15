package scalus.cardano.node.stream

import scalus.cardano.ledger.{Transaction, TransactionHash}
import scalus.cardano.node.SubmitError

import scala.concurrent.Future

/** Capability surface of an N2N `TxSubmission2`-backed submit path. Narrower than
  * [[scalus.cardano.node.BlockchainProvider]] and [[LocalNodeBackend]] because the protocol exposes
  * *only* submit — TxSubmission2 has no read or query side.
  *
  * Acceptance semantics: [[submit]] always returns `Right(transaction.id)` once the tx is enqueued
  * for the peer to pull. TxSubmission2 has no `MsgRejectTx`, so the wire never carries a typed
  * acceptance/rejection signal — the only `Left` is `ConnectionError` if the driver is closed or
  * the underlying connection has dropped. Apps that need a binary confirmed/timeout signal use
  * [[BaseStreamProvider.submitAndPoll]] with `confirmations > 0`; apps that need *typed* reject
  * reasons use Blockfrost or N2C `LocalTxSubmission` instead.
  *
  * See `n2n-txsubmission2-m8.md` § *Submit semantics* and `BaseStreamProvider.submit`'s routing
  * precedence (Blockfrost > LocalNode > N2N > none).
  */
trait N2nTxSubmissionBackend {

    /** Enqueue `transaction` for the peer to pull. Returns `Right(transaction.id)` immediately on
      * enqueue; `Left(ConnectionError)` only on driver-level failures (closed, dropped connection).
      */
    def submit(transaction: Transaction): Future[Either[SubmitError, TransactionHash]]

    /** Stop accepting new submits; release the queue. The underlying mini-protocol channel is torn
      * down separately by the provider when the `NodeToNodeConnection` closes. Idempotent.
      */
    def close(): Future[Unit]
}
