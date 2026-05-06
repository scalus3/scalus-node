package scalus.cardano.node.stream.engine

import scalus.cardano.ledger.{Block, DataHash, DatumOption, KeepRaw, OriginalCborByteArray, TransactionHash, TransactionInput, TransactionOutput}
import scalus.cardano.node.stream.{ChainPoint, ChainTip}
import scalus.uplc.builtin.Data

/** Engine-internal view of a block that has been parsed enough to be applied to the UTxO set.
  * Decouples the engine from block CBOR and witness-set details we don't care about for indexing.
  *
  * `spent` and `created` are pre-extracted from the transactions to keep the engine's inner loops
  * free of per-tx destructuring.
  *
  * `datums` is the union of inline output datums and witness-set `plutusData` entries observed in
  * this block, keyed by the on-chain `DataHash`. The engine consumes it to maintain an in-memory
  * `DataHash -> Data` index that backs `BlockchainReader.getDatum` for in-window hits.
  */
final case class AppliedBlock(
    tip: ChainTip,
    transactions: Seq[AppliedTransaction],
    datums: Map[DataHash, Data] = Map.empty
) {
    def point: ChainPoint = tip.point

    lazy val spent: Set[TransactionInput] =
        transactions.iterator.flatMap(_.inputs).toSet

    lazy val created: Map[TransactionInput, TransactionOutput] =
        transactions.iterator.flatMap(_.outputsById).toMap

    lazy val transactionIds: Set[TransactionHash] =
        transactions.iterator.map(_.id).toSet
}

object AppliedBlock {

    /** Project a decoded [[Block]] (with its original CBOR bytes retained) into the engine's
      * [[AppliedBlock]] shape. The caller owns the `blockRaw.raw` bytes and supplies the tip
      * separately — for network paths that's the peer's reported tip, for snapshot-restore paths
      * it's typically reconstructed from the immutable index.
      *
      * Rebinds `OriginalCborByteArray` from `blockRaw.raw` so each transaction's `KeepRaw` parts
      * resolve their backing slice correctly — same discipline as
      * `scalus.cardano.network.ChainApplier.toAppliedBlock`, kept in one place so both the N2N
      * applier and the Mithril snapshot restorer produce identical results.
      */
    def fromRaw(tip: ChainTip, blockRaw: KeepRaw[Block]): AppliedBlock = {
        given OriginalCborByteArray = OriginalCborByteArray(blockRaw.raw)
        val datums = scala.collection.mutable.Map.empty[DataHash, Data]
        val txs = blockRaw.value.transactions.map { tx =>
            collectDatums(tx, datums)
            AppliedTransaction(
              id = tx.id,
              inputs = tx.body.value.inputs.toSet,
              outputs = tx.body.value.outputs.map(_.value).toIndexedSeq
            )
        }
        AppliedBlock(tip, txs, datums.toMap)
    }

    /** Append a transaction's inline output datums and witness-set `plutusData` entries to `into`,
      * keyed by [[DataHash]]. Shared between [[fromRaw]] and the synthetic-emulator path so both
      * surfaces feed the engine's in-memory datum index identically.
      */
    private[engine] def collectDatums(
        tx: scalus.cardano.ledger.Transaction,
        into: scala.collection.mutable.Map[DataHash, Data]
    ): Unit = {
        tx.witnessSet.plutusData.value.toSortedMap.foreach { (h, d) =>
            into.getOrElseUpdate(h, d.value)
        }
        tx.body.value.outputs.foreach { sized =>
            sized.value.datumOption match {
                case Some(DatumOption.Inline(d)) =>
                    into.getOrElseUpdate(DataHash.fromByteString(d.dataHash), d)
                case _ => ()
            }
        }
    }
}

final case class AppliedTransaction(
    id: TransactionHash,
    inputs: Set[TransactionInput],
    outputs: IndexedSeq[TransactionOutput]
) {
    def outputsById: Iterator[(TransactionInput, TransactionOutput)] =
        outputs.iterator.zipWithIndex.map((o, i) => TransactionInput(id, i) -> o)
}
