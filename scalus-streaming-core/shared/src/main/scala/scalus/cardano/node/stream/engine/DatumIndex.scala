package scalus.cardano.node.stream.engine

import scalus.cardano.ledger.DataHash
import scalus.cardano.node.stream.ChainPoint
import scalus.uplc.builtin.Data

import scala.collection.mutable

/** In-memory `DataHash -> Data` index over the engine's volatile tail. Populated alongside the
  * [[RollbackBuffer]] from inline datums and witness-set `plutusData` observed in each applied
  * block; pruned when a block is evicted or rolled back so the index never holds an entry for a
  * datum that is no longer reachable from the volatile chain.
  *
  * Datum hashes that appear in multiple blocks are reference-counted: a hash is removed from the
  * index only when the last block that introduced it leaves the volatile tail.
  *
  * Not thread-safe. The engine owns a single-threaded `ExecutionContext` that serialises all
  * mutating calls — same discipline as [[RollbackBuffer]] / [[TxHashIndex]].
  */
final class DatumIndex {

    private val data = mutable.Map.empty[DataHash, (Data, Int)]
    private val perBlock = mutable.ArrayDeque.empty[(ChainPoint, Set[DataHash])]

    /** Append the datums introduced by a block. Called from the engine's `onRollForward` after the
      * rollback buffer has accepted the block, so the index and the buffer stay in lockstep.
      *
      * Empty `datums` is still recorded so subsequent `forgetBlock` / `applyBackward` calls find the
      * block at the expected end of the deque.
      */
    def applyForward(point: ChainPoint, datums: Map[DataHash, Data]): Unit = {
        perBlock += (point -> datums.keySet)
        datums.foreach { (h, d) =>
            data.updateWith(h) {
                case Some((existing, n)) => Some((existing, n + 1))
                case None                => Some((d, 1))
            }
        }
    }

    /** Drop the index contributions of the block at `point`, if it sits at the immutable end of the
      * tracked window. Mirrors [[TxHashIndex.forgetBlock]]; a no-op if `point` isn't at the head.
      */
    def forgetBlock(point: ChainPoint): Unit =
        if perBlock.nonEmpty && perBlock.head._1 == point then decrement(perBlock.removeHead()._2)

    /** Drop the index contributions of the most recently applied block. Mirrors
      * [[TxHashIndex.applyBackward]]; called once per reverted block during a rollback.
      */
    def applyBackward(): Unit =
        if perBlock.nonEmpty then decrement(perBlock.removeLast()._2)

    /** Look up a datum witnessed in the volatile tail. Returns `None` if no block in the current
      * window contributed this hash; callers fall through to the historical-backup reader.
      */
    def get(hash: DataHash): Option[Data] = data.get(hash).map(_._1)

    /** Drop all tracked state. Used by the engine on a past-horizon rollback when every other piece
      * of state is also being torn down.
      */
    def clear(): Unit = {
        data.clear()
        perBlock.clear()
    }

    private[engine] def trackedHashes: Int = data.size

    private def decrement(keys: Set[DataHash]): Unit =
        keys.foreach { h =>
            data.updateWith(h) {
                case Some((_, 1)) => None
                case Some((d, n)) => Some((d, n - 1))
                case None         => None
            }
        }
}
