package scalus.cardano.network.n2c.handshake

/** NodeToClient protocol versions. The wire numbering is shared across the Cardano Haskell stack —
  * each value bakes in compatibility with a specific era's CBOR shape.
  *
  * M11 targets v16+ (Conway-era). Older versions still negotiate on the wire, but their
  * version-data shape differs and we deliberately do not propose them. M12's LSQ work flips
  * `query = true` in the proposed version data; M11 always proposes `query = false`.
  */
object NodeToClientVersion {
    val V16: Int = 16
    val V17: Int = 17
    val V18: Int = 18
    val V19: Int = 19
    val V20: Int = 20
}
