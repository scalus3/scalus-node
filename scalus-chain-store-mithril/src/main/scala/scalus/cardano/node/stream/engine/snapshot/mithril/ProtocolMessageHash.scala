package scalus.cardano.node.stream.engine.snapshot.mithril

import scalus.utils.Hex.toHex

import java.security.MessageDigest

/** SHA-256 over the parts of a Mithril `ProtocolMessage`, computed exactly the way upstream
  * `mithril-common::entities::ProtocolMessage::compute_hash` does:
  *
  *   - iterate the parts in **enum-declaration order** of [[ProtocolMessagePartKey]] (NOT
  *     lex-by-key);
  *   - for each `(key, value)` update the hasher with the snake_case key bytes, then with the
  *     value bytes;
  *   - output is lowercase hex.
  *
  * The result is what every Mithril certificate puts in its `signed_message` field, so a successful
  * comparison `ProtocolMessageHash.compute(msg) == cert.signedMessage` proves the parts of `msg`
  * are exactly what the aggregator's threshold signature attests to.
  */
object ProtocolMessageHash {

    /** SHA-256 hex of `parts` iterated in [[ProtocolMessagePartKey]] declaration order. Throws if
      * `parts` contains a key not in the canonical enum — that would silently change which parts
      * contribute to the hash and is almost certainly a sign of a wire-shape change we need to
      * handle deliberately rather than tolerate.
      */
    def compute(parts: Map[String, String]): String = {
        val md = MessageDigest.getInstance("SHA-256")
        parts.toSeq
            .sortBy { (k, _) =>
                ProtocolMessagePartKey
                    .fromSnakeCase(k)
                    .getOrElse(
                      throw new IllegalArgumentException(
                        s"unknown ProtocolMessagePartKey '$k' — extend " +
                            "ProtocolMessagePartKey to match upstream Mithril"
                      )
                    )
                    .ordinal
            }
            .foreach { (k, v) =>
                md.update(k.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                md.update(v.getBytes(java.nio.charset.StandardCharsets.UTF_8))
            }
        md.digest().toHex
    }

    /** Convenience overload taking a [[MithrilMessages.ProtocolMessage]] directly. */
    def compute(msg: MithrilMessages.ProtocolMessage): String = compute(msg.messageParts)
}
