package scalus.cardano.node.stream.engine.snapshot.mithril

import org.bouncycastle.crypto.digests.Blake2sDigest
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets.US_ASCII

/** Sanity checks for the Blake2s256-based MMR. The trivial cases are verifiable from first
  * principles; the larger-input cases just check determinism + that distinct inputs produce
  * distinct roots. Cross-validation against the Rust `ckb-merkle-mountain-range` impl happens
  * implicitly the first time we point the verifier at a real Mithril snapshot — a wrong MMR root
  * would cause `signed_message` comparison to fail every time, which we'd notice immediately.
  *
  * Leaf strings are encoded with US-ASCII rather than the platform default so the produced roots
  * are reproducible across JVMs/locales.
  */
final class MerkleMountainRangeSuite extends AnyFunSuite {

    test("single leaf: root equals leaf bytes verbatim (no hashing)") {
        val leaf = "aa".getBytes(US_ASCII)
        val root = MerkleMountainRange.computeRoot(Seq(leaf))
        assert(root.toSeq == leaf.toSeq)
    }

    test("two leaves: root = blake2s256(l0 || l1)") {
        val l0 = "aa".getBytes(US_ASCII)
        val l1 = "bb".getBytes(US_ASCII)
        val root = MerkleMountainRange.computeRoot(Seq(l0, l1))
        assert(root.toSeq == manualBlake2s256(l0, l1).toSeq)
    }

    test("four leaves: balanced tree of one height-2 peak") {
        val leaves = Seq("aa", "bb", "cc", "dd").map(_.getBytes(US_ASCII))
        val left = manualBlake2s256(leaves(0), leaves(1))
        val right = manualBlake2s256(leaves(2), leaves(3))
        val expected = manualBlake2s256(left, right)
        assert(MerkleMountainRange.computeRoot(leaves).toSeq == expected.toSeq)
    }

    test("three leaves: bag combines [P_1 (h=1), P_0 (h=0)] via H(acc, left)") {
        // ckb's bagging starts at the rightmost peak (P_0) and merges in each leftward peak via
        // H(acc, left). For 3 leaves: peaks = [P_1, P_0]; acc = P_0; acc = H(P_0, P_1).
        val leaves = Seq("aa", "bb", "cc").map(_.getBytes(US_ASCII))
        val p1 = manualBlake2s256(leaves(0), leaves(1))
        val p0 = leaves(2)
        val expected = manualBlake2s256(p0, p1)
        assert(MerkleMountainRange.computeRoot(leaves).toSeq == expected.toSeq)
    }

    test("five leaves: peaks {h=2, h=0} bagged via H(acc, left)") {
        // 5 = 0b101 → peaks [P_2, P_0]; acc = P_0; acc = H(P_0, P_2).
        val leaves = Seq("aa", "bb", "cc", "dd", "ee").map(_.getBytes(US_ASCII))
        val q1a = manualBlake2s256(leaves(0), leaves(1))
        val q1b = manualBlake2s256(leaves(2), leaves(3))
        val p2 = manualBlake2s256(q1a, q1b)
        val p0 = leaves(4)
        val expected = manualBlake2s256(p0, p2)
        assert(MerkleMountainRange.computeRoot(leaves).toSeq == expected.toSeq)
    }

    test("seven leaves: peaks {h=2, h=1, h=0} bagged via H(acc, left) walking right-to-left") {
        // 7 = 0b111 → peaks [P_2, P_1, P_0]; walk right-to-left:
        //   acc = P_0
        //   acc = H(P_0, P_1)
        //   acc = H(H(P_0, P_1), P_2)
        val ls = Seq("a", "b", "c", "d", "e", "f", "g").map(_.getBytes(US_ASCII))
        val q1a = manualBlake2s256(ls(0), ls(1))
        val q1b = manualBlake2s256(ls(2), ls(3))
        val p2 = manualBlake2s256(q1a, q1b)
        val p1 = manualBlake2s256(ls(4), ls(5))
        val p0 = ls(6)
        val step1 = manualBlake2s256(p0, p1)
        val expected = manualBlake2s256(step1, p2)
        assert(MerkleMountainRange.computeRoot(ls).toSeq == expected.toSeq)
    }

    /** Lifted verbatim from
      * `mithril-common/src/crypto_helper/merkle_tree.rs::test_golden_merkle_root`. If our MMR
      * matches upstream's `MKTree`, this hex must match byte-for-byte.
      */
    test("upstream golden vector: leaves=[golden-1..golden-5]") {
        val leaves = Seq("golden-1", "golden-2", "golden-3", "golden-4", "golden-5")
            .map(_.getBytes(US_ASCII))
        val expectedHex =
            "3bbced153528697ecde7345a22e50115306478353619411523e804f2323fd921"
        val rootHex = bytesToHex(MerkleMountainRange.computeRoot(leaves))
        assert(rootHex == expectedHex, s"\nexpected: $expectedHex\nactual:   $rootHex")
    }

    test("computeRoot is deterministic across runs") {
        val leaves = (0 until 100).map(i => f"$i%08x".getBytes(US_ASCII))
        val r1 = MerkleMountainRange.computeRoot(leaves).toSeq
        val r2 = MerkleMountainRange.computeRoot(leaves).toSeq
        assert(r1 == r2)
    }

    test("distinct inputs produce distinct roots (collision sanity)") {
        val a = (0 until 50).map(i => f"$i%04x".getBytes(US_ASCII))
        val b = (0 until 50).map(i => f"${i + 1}%04x".getBytes(US_ASCII))
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

    private def bytesToHex(bs: Array[Byte]): String = {
        val sb = new java.lang.StringBuilder(bs.length * 2)
        var i = 0
        while i < bs.length do {
            val b = bs(i) & 0xff
            sb.append(Character.forDigit((b >>> 4) & 0xf, 16))
            sb.append(Character.forDigit(b & 0xf, 16))
            i += 1
        }
        sb.toString
    }
}
