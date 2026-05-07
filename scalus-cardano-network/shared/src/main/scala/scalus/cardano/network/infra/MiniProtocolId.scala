package scalus.cardano.network.infra

/** Ouroboros mini-protocol identifiers, carried in the low 15 bits of the mux SDU header's protocol
  * field. The enum variants correspond 1:1 to the wire numbers defined in ouroboros-network's
  * `network-mux` / `cardano-diffusion` protocol tables.
  *
  * Both Node-to-Node (N2N) and Node-to-Client (N2C) protocols share the same enum; only
  * [[Handshake]] is shared on the wire (id 0). The remaining N2N variants ([[ChainSync]],
  * [[BlockFetch]], [[TxSubmission]], [[KeepAlive]], [[PeerSharing]]) and N2C variants
  * ([[LocalChainSync]], [[LocalTxSubmission]], [[LocalStateQuery]], [[LocalTxMonitor]]) carry
  * distinct wire ids and a given connection only mounts handlers for one transport's subset.
  */
enum MiniProtocolId(val wire: Int) {
    case Handshake extends MiniProtocolId(0)
    case ChainSync extends MiniProtocolId(2)
    case BlockFetch extends MiniProtocolId(3)
    case TxSubmission extends MiniProtocolId(4)
    case LocalChainSync extends MiniProtocolId(5)
    case LocalTxSubmission extends MiniProtocolId(6)
    case LocalStateQuery extends MiniProtocolId(7)
    case KeepAlive extends MiniProtocolId(8)
    case LocalTxMonitor extends MiniProtocolId(9)
    case PeerSharing extends MiniProtocolId(10)
}

object MiniProtocolId {

    /** Lookup by wire number. Returns `None` for unknown protocols — the mux treats an unknown
      * inbound protocol-ID as a wire-level failure and fires the connection-root cancel.
      */
    def fromWire(wire: Int): Option[MiniProtocolId] = values.find(_.wire == wire)
}
