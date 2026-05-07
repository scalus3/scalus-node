package scalus.cardano.node.stream.engine

import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.node.stream.ChainPoint
import scalus.cardano.node.stream.engine.kvstore.InMemoryKvStore

import EngineTestFixtures.*

class KvChainStoreDatumDictSuite extends AnyFunSuite {

    private def newStore(): KvChainStore = new KvChainStore(InMemoryKvStore())

    test("appendBlock persists block.datums; getDatumFromStore round-trips") {
        val store = newStore()
        val (h1, d1) = datum(1)
        val (h2, d2) = datum(2)
        val blk = block(1, tx(100)).copy(datums = Map(h1 -> d1, h2 -> d2))

        store.appendBlock(blk)
        assert(store.getDatumFromStore(h1).contains(d1))
        assert(store.getDatumFromStore(h2).contains(d2))
    }

    test("missing hash returns None") {
        val store = newStore()
        val (h, d) = datum(7)
        store.appendBlock(block(1, tx(100)).copy(datums = Map(h -> d)))
        val (other, _) = datum(8)
        assert(store.getDatumFromStore(other).isEmpty)
    }

    test("dict is content-addressed and monotonic across rollbacks") {
        // Datums committed by a rolled-back block remain in the dict — they're keyed by hash, so
        // the value is still correct if the same hash reappears on another fork. This is the
        // documented semantics of ChainStoreDatumDict.
        val store = newStore()
        val (h, d) = datum(42)
        store.appendBlock(block(1, tx(100)).copy(datums = Map(h -> d)))
        store.appendBlock(block(2, tx(101)))
        store.appendBlock(block(3, tx(102)))

        store.rollbackTo(ChainPoint.origin)
        assert(store.tip.isEmpty)
        assert(store.getDatumFromStore(h).contains(d))
    }

    test("appendBlock is idempotent on the datum dict (same hash + value)") {
        val store = newStore()
        val (h, d) = datum(123)
        val blk = block(1, tx(100)).copy(datums = Map(h -> d))
        store.appendBlock(blk)
        store.appendBlock(blk)
        assert(store.getDatumFromStore(h).contains(d))
    }

    test("multiple blocks contribute to a single dict") {
        val store = newStore()
        val (h1, d1) = datum(1)
        val (h2, d2) = datum(2)
        store.appendBlock(block(1, tx(100)).copy(datums = Map(h1 -> d1)))
        store.appendBlock(block(2, tx(101)).copy(datums = Map(h2 -> d2)))

        assert(store.getDatumFromStore(h1).contains(d1))
        assert(store.getDatumFromStore(h2).contains(d2))
    }
}
