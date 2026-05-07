package scalus.cardano.network.n2c

import scalus.cardano.infra.CancelToken
import scalus.cardano.network.infra.{MiniProtocolBytes, MiniProtocolId}
import scalus.cardano.network.n2c.handshake.NegotiatedVersion

import scala.concurrent.Future

/** A live Node-to-Client connection: post-handshake, mini-protocol byte channels available.
  *
  * The connection is obtained via the JVM-only factory
  * `scalus.cardano.network.n2c.NodeToClientClient.connect`. Unlike N2N, there is no keep-alive
  * mini-protocol on N2C — the local cardano-node and the in-process client are typically on the
  * same host and idle connections do not get reaped by intermediate routers.
  *
  * Lifecycle is observed via [[rootToken]] / [[closed]]; teardown via [[close]].
  */
trait NodeToClientConnection {

    /** The version negotiated during handshake (>= v16 for M11). */
    def negotiatedVersion: NegotiatedVersion

    /** Per-mini-protocol byte-stream handle. Calls for the same `proto` return the same handle —
      * idempotent. M11 uses [[MiniProtocolId.Handshake]] internally during connect; the chain-sync
      * and local-tx-submission state machines (M11.P2 / M11.P3) consume the corresponding handles.
      */
    def channel(proto: MiniProtocolId): MiniProtocolBytes

    /** Observable root token — cancelled on any connection-fatal event (peer EOF, socket error,
      * explicit [[close]]). The cause is stored on the token and is also the failure value of
      * [[closed]].
      */
    def rootToken: CancelToken

    /** Completes when the connection closes. Succeeds on user-initiated [[close]] or clean peer
      * EOF; fails with the stored cause on any fault path.
      */
    def closed: Future[Unit]

    /** Tear down the connection. Fires the connection root and closes the underlying socket; the
      * returned future completes when cleanup finishes.
      */
    def close(): Future[Unit]
}
