package scalus.cardano.node.stream.engine.snapshot.mithril

import org.bouncycastle.crypto.digests.Blake2sDigest

/** Merkle Mountain Range (MMR) — port of upstream `mithril-common::crypto_helper::MKTree`, which
  * is itself a thin wrapper over `ckb-merkle-mountain-range` with **Blake2s256** as the merge
  * primitive. Just enough functionality to compute a root over a list of leaves; we don't need
  * proof generation or membership checks for the verifier.
  *
  * ==Layout==
  *
  * Leaves are appended left-to-right and combined into peaks. Whenever the top two peaks have
  * equal height, they're merged into one of height+1. The MMR's "root" is the sequence of
  * surviving peaks bagged from right to left:
  *
  * {{{
  * peaks = [P_k, P_{k-1}, ..., P_1, P_0]   // left-to-right, ascending heights only at boundaries
  * root  = bag(P_k, bag(P_{k-1}, bag(... bag(P_1, P_0)) ...))
  * bag(a, b) = blake2s256(a || b)
  * }}}
  *
  * For a single leaf, the root is the leaf bytes directly (no hashing). For two leaves, it's
  * `blake2s256(leaf0 || leaf1)`. Matches the canonical Peter-Todd MMR with right-to-left bagging.
  *
  * ==Leaf encoding==
  *
  * Per upstream's `From<&str> for MKTreeNode`, a hex-encoded SHA-256 digest enters the tree as
  * the **ASCII bytes of the hex string** (e.g. `"deadbeef"` → 8 bytes), NOT as the 32 raw bytes.
  * Callers building the verifier need to hand in the hex strings byte-for-byte.
  */
object MerkleMountainRange {

    /** Build the MMR over `leaves` and return the root bytes. Throws on empty input — an empty
      * MMR has no defined root in upstream either.
      */
    def computeRoot(leaves: Seq[Array[Byte]]): Array[Byte] = {
        require(leaves.nonEmpty, "MerkleMountainRange requires at least one leaf")
        val peaks = scala.collection.mutable.ArrayBuffer.empty[(Int, Array[Byte])]
        leaves.foreach { leaf =>
            var node = leaf
            var height = 0
            while peaks.nonEmpty && peaks.last._1 == height do {
                val (_, left) = peaks.remove(peaks.length - 1)
                node = blake2s256(left, node)
                height += 1
            }
            peaks += ((height, node))
        }
        bagPeaksRightToLeft(peaks.toSeq.map(_._2))
    }

    /** Bag peaks the way `ckb-merkle-mountain-range::bagging_peaks_hashes` does: start with the
      * rightmost peak as the accumulator, then walk right-to-left merging in each remaining peak
      * with the **accumulator first** in the hash input — `H(acc || left_peak)`, not the
      * `H(left_peak || acc)` you'd get from a naïve "fold-from-the-right" reading.
      */
    private def bagPeaksRightToLeft(peaks: Seq[Array[Byte]]): Array[Byte] = {
        require(peaks.nonEmpty)
        var acc = peaks.last
        var i = peaks.length - 2
        while i >= 0 do {
            acc = blake2s256(acc, peaks(i))
            i -= 1
        }
        acc
    }

    /** Lightweight BouncyCastle API — no JCA provider registration. */
    private def blake2s256(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
        val d = new Blake2sDigest(256)
        d.update(a, 0, a.length)
        d.update(b, 0, b.length)
        val out = new Array[Byte](32)
        d.doFinal(out, 0)
        out
    }
}
