package scalus.cardano.node.stream.engine

import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.node.TransactionStatus

import EngineTestFixtures.*

class TxHashIndexSuite extends AnyFunSuite {

    test("applyForward records transactions as Confirmed") {
        val index = new TxHashIndex
        index.applyForward(block(1, tx(10), tx(11)))
        assert(index.statusOf(txHash(10)).contains(TransactionStatus.Confirmed))
        assert(index.statusOf(txHash(11)).contains(TransactionStatus.Confirmed))
        assert(index.statusOf(txHash(99)).isEmpty)
    }

    test("recordOwnSubmission reports Pending until confirmed") {
        val index = new TxHashIndex
        index.recordOwnSubmission(txHash(42))
        assert(index.statusOf(txHash(42)).contains(TransactionStatus.Pending))
        index.applyForward(block(1, tx(42)))
        assert(index.statusOf(txHash(42)).contains(TransactionStatus.Confirmed))
    }

    test("applyBackward forgets confirmations from the last block only") {
        val index = new TxHashIndex
        index.applyForward(block(1, tx(10)))
        index.applyForward(block(2, tx(20)))
        index.applyBackward()
        assert(index.statusOf(txHash(10)).contains(TransactionStatus.Confirmed))
        assert(index.statusOf(txHash(20)).isEmpty)
    }

    test("confirmationTip returns the tip carrying blockNo (for depth math)") {
        val index = new TxHashIndex
        index.applyForward(block(7, tx(42)))
        val tip = index.confirmationTip(txHash(42))
        assert(tip.isDefined)
        assert(tip.get.point == point(7))
        // EngineTestFixtures.block(slot, ...) builds a ChainTip whose blockNo == slot. Depth math
        // in Engine.confirmationDepth uses this same blockNo.
        assert(tip.get.blockNo == 7L)
        // No confirmation tip for unknown tx.
        assert(index.confirmationTip(txHash(99)).isEmpty)
    }

    test("forgetBlock drops the per-block journal but keeps Confirmed status") {
        val index = new TxHashIndex
        index.applyForward(block(1, tx(10)))
        index.forgetBlock(point(1))
        // The confirmation entry is still there (the tx is still
        // Confirmed from the app's perspective); only the rollback
        // ability for that block is gone.
        assert(index.statusOf(txHash(10)).contains(TransactionStatus.Confirmed))
    }
}
