package scalus.cardano.node.stream.engine.snapshot.mithril

import org.bouncycastle.crypto.digests.Blake2sDigest
import org.scalatest.funsuite.AnyFunSuite

/** Sanity checks for the Blake2s256-based MMR. The trivial cases are verifiable from first
  * principles; the larger-input cases just check determinism + that distinct inputs produce
  * distinct roots. Cross-validation against the Rust `ckb-merkle-mountain-range` impl happens
  * implicitly the first time we point the verifier at a real Mithril snapshot — a wrong MMR root
  * would cause `signed_message` comparison to fail every time, which we'd notice immediately.
  */
final class MerkleMountainRangeSuite extends AnyFunSuite {

    test("single leaf: root equals leaf bytes verbatim (no hashing)") {
        val leaf = "aa".getBytes
        val root = MerkleMountainRange.computeRoot(Seq(leaf))
        assert(root.toSeq == leaf.toSeq)
    }

    test("two leaves: root = blake2s256(l0 || l1)") {
        val l0 = "aa".getBytes
        val l1 = "bb".getBytes
        val root = MerkleMountainRange.computeRoot(Seq(l0, l1))
        assert(root.toSeq == manualBlake2s256(l0, l1).toSeq)
    }

    test("four leaves: balanced tree of one height-2 peak") {
        val leaves = Seq("aa", "bb", "cc", "dd").map(_.getBytes)
        val left = manualBlake2s256(leaves(0), leaves(1))
        val right = manualBlake2s256(leaves(2), leaves(3))
        val expected = manualBlake2s256(left, right)
        assert(MerkleMountainRange.computeRoot(leaves).toSeq == expected.toSeq)
    }

    test("three leaves: bag-the-peaks combines a height-1 and a height-0 right-to-left") {
        val leaves = Seq("aa", "bb", "cc").map(_.getBytes)
        val p1 = manualBlake2s256(leaves(0), leaves(1))
        val p0 = leaves(2)
        val expected = manualBlake2s256(p1, p0)
        assert(MerkleMountainRange.computeRoot(leaves).toSeq == expected.toSeq)
    }

    test("five leaves: bag right-to-left of [P_2 (h=2), P_0 (h=0)]") {
        // 5 = 0b101 → two surviving peaks of heights {2, 0}.
        val leaves = Seq("aa", "bb", "cc", "dd", "ee").map(_.getBytes)
        val q1a = manualBlake2s256(leaves(0), leaves(1))
        val q1b = manualBlake2s256(leaves(2), leaves(3))
        val p2 = manualBlake2s256(q1a, q1b)
        val p0 = leaves(4)
        val expected = manualBlake2s256(p2, p0)
        assert(MerkleMountainRange.computeRoot(leaves).toSeq == expected.toSeq)
    }

    test("seven leaves: peaks {h=2, h=1, h=0} bagged right-to-left") {
        // 7 = 0b111
        val ls = Seq("a", "b", "c", "d", "e", "f", "g").map(_.getBytes)
        val q1a = manualBlake2s256(ls(0), ls(1))
        val q1b = manualBlake2s256(ls(2), ls(3))
        val p2 = manualBlake2s256(q1a, q1b)
        val p1 = manualBlake2s256(ls(4), ls(5))
        val p0 = ls(6)
        val expected = manualBlake2s256(p2, manualBlake2s256(p1, p0))
        assert(MerkleMountainRange.computeRoot(ls).toSeq == expected.toSeq)
    }

    test("computeRoot is deterministic across runs") {
        val leaves = (0 until 100).map(i => f"$i%08x".getBytes)
        val r1 = MerkleMountainRange.computeRoot(leaves).toSeq
        val r2 = MerkleMountainRange.computeRoot(leaves).toSeq
        assert(r1 == r2)
    }

    test("distinct inputs produce distinct roots (collision sanity)") {
        val a = (0 until 50).map(i => f"$i%04x".getBytes)
        val b = (0 until 50).map(i => f"${i + 1}%04x".getBytes)
        val ra = MerkleMountainRange.computeRoot(a).toSeq
        val rb = MerkleMountainRange.computeRoot(b).toSeq
        assert(ra != rb)
    }

    test("empty input is rejected") {
        intercept[IllegalArgumentException](MerkleMountainRange.computeRoot(Seq.empty))
    }

    private def manualBlake2s256(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
        val d = new Blake2sDigest(256)
        d.update(a, 0, a.length)
        d.update(b, 0, b.length)
        val out = new Array[Byte](32)
        d.doFinal(out, 0)
        out
    }
}
