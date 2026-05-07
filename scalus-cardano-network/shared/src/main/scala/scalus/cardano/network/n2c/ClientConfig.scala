package scalus.cardano.network.n2c

import scalus.cardano.network.infra.Sdu
import scalus.cardano.node.stream.DeltaBufferPolicy

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** Tunables for the N2C client. Mirrors [[scalus.cardano.network.ClientConfig]] but without the
  * keep-alive knobs — N2C has no keep-alive mini-protocol.
  *
  * @param handshakeTimeout
  *   maximum time from socket-open to handshake completion. On expiry the returned connection
  *   future fails with [[handshake.HandshakeError.Timeout]].
  * @param query
  *   `query` flag advertised in the handshake's version-data. M11 leaves this at `false`; M12's
  *   LocalStateQuery work raises it to `true`.
  * @param sduMaxPayload
  *   limit on the payload portion of a single SDU.
  * @param mailboxCapacity
  *   per-route mailbox capacity.
  */
final case class ClientConfig(
    handshakeTimeout: FiniteDuration = 30.seconds,
    query: Boolean = false,
    sduMaxPayload: Int = Sdu.MaxPayloadSize,
    mailboxCapacity: DeltaBufferPolicy = DeltaBufferPolicy.Bounded(256)
)

object ClientConfig {
    val default: ClientConfig = ClientConfig()
}
