package scalus.cardano.node.stream.engine

import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.ledger.DataHash
import scalus.uplc.builtin.Data
import scalus.uplc.builtin.Data.dataHash

import EngineTestFixtures.point

class DatumIndexSuite extends AnyFunSuite {

    private def datum(n: Int): (DataHash, Data) = {
        val d: Data = Data.I(BigInt(n))
        DataHash.fromByteString(d.dataHash) -> d
    }

    test("forward + lookup hits, backward removes") {
        val idx = new DatumIndex
        val (h1, d1) = datum(1)
        idx.applyForward(point(1), Map(h1 -> d1))
        assert(idx.get(h1).contains(d1))

        idx.applyBackward()
        assert(idx.get(h1).isEmpty)
        assert(idx.trackedHashes == 0)
    }

    test("hash present in two blocks survives until both are gone") {
        val idx = new DatumIndex
        val (h, d) = datum(42)
        idx.applyForward(point(1), Map(h -> d))
        idx.applyForward(point(2), Map(h -> d))
        assert(idx.get(h).contains(d))

        idx.forgetBlock(point(1))
        assert(idx.get(h).contains(d))

        idx.applyBackward()
        assert(idx.get(h).isEmpty)
    }

    test("forgetBlock only matches the oldest tracked block") {
        val idx = new DatumIndex
        val (h1, d1) = datum(1)
        val (h2, d2) = datum(2)
        idx.applyForward(point(1), Map(h1 -> d1))
        idx.applyForward(point(2), Map(h2 -> d2))

        idx.forgetBlock(point(2))
        assert(idx.get(h1).contains(d1))
        assert(idx.get(h2).contains(d2))
    }

    test("clear drops every tracked datum and per-block bookkeeping") {
        val idx = new DatumIndex
        val (h1, d1) = datum(1)
        val (h2, d2) = datum(2)
        idx.applyForward(point(1), Map(h1 -> d1, h2 -> d2))
        idx.clear()
        assert(idx.get(h1).isEmpty)
        assert(idx.get(h2).isEmpty)
        assert(idx.trackedHashes == 0)
    }
}
