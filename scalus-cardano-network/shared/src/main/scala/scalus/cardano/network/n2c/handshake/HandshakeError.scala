package scalus.cardano.network.n2c.handshake

/** Errors raised by the N2C [[HandshakeDriver]]. Mirrors the N2N error model — see
  * `scalus.cardano.network.handshake.HandshakeError` — with the only difference that
  * [[UnexpectedMessage.received]] carries an N2C [[HandshakeMessage]] rather than the N2N one.
  */
sealed abstract class HandshakeError(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

object HandshakeError {

    final class VersionMismatch(val proposed: Set[Int], val peerSupported: List[Int])
        extends HandshakeError(
          s"n2c handshake version mismatch: we proposed " +
              s"${proposed.toSeq.sorted.mkString("[", ",", "]")}, " +
              s"peer supports ${peerSupported.mkString("[", ",", "]")}"
        )

    final class Refused(val versionTried: Int, val reason: String)
        extends HandshakeError(s"n2c handshake refused at v$versionTried: $reason")

    final class DecodeError(message: String, cause: Throwable)
        extends HandshakeError(s"n2c handshake decode error: $message", cause)

    final class Timeout extends HandshakeError("n2c handshake timed out")

    final class UnexpectedMessage(val received: HandshakeMessage)
        extends HandshakeError(s"unexpected n2c handshake message from peer: $received")
}
