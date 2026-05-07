package scalus.cardano.network.n2c.handshake

import scalus.cardano.infra.{CancelSource, CancelledException, Timer}
import scalus.cardano.network.NetworkMagic
import scalus.cardano.network.infra.{CborMessageStream, FrameDecodeException, MiniProtocolBytes, MiniProtocolId}
import scalus.cardano.network.n2c.handshake.HandshakeMessage.*

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Outcome of a successful N2C handshake: the version the peer accepted and the version-data they
  * sent back.
  */
final case class NegotiatedVersion(version: Int, data: NodeToClientVersionData)

/** Initiator-side driver for the N2C handshake mini-protocol (id 0). One-shot: call [[run]] once
  * per connection, receive a `Future[NegotiatedVersion]`, discard.
  *
  * Mirrors the N2N [[scalus.cardano.network.handshake.HandshakeDriver]] state machine and timeout
  * policy verbatim — only the proposal table and message ADT differ. The default proposal lists
  * v16..v20; the highest mutually supported version wins per the standard handshake semantics.
  *
  * `query = false` is sent in every proposed version-data: M11 does not use LSQ. M12 will raise
  * this and pass `query = true`.
  */
object HandshakeDriver {

    private val defaultLogger: scribe.Logger =
        scribe.Logger("scalus.cardano.network.n2c.handshake")

    def run(
        handle: MiniProtocolBytes,
        magic: NetworkMagic,
        cancelScope: CancelSource,
        timer: Timer,
        timeout: FiniteDuration = 30.seconds,
        query: Boolean = false,
        logger: scribe.Logger = defaultLogger
    )(using ExecutionContext): Future[NegotiatedVersion] = {
        val table = defaultProposal(magic, query)
        val stream = new CborMessageStream[HandshakeMessage](MiniProtocolId.Handshake, handle)

        val scheduled = timer.schedule(timeout) {
            cancelScope.cancel(new HandshakeError.Timeout)
        }

        val exchange = stream
            .send(MsgProposeVersions(table), cancelScope.token)
            .flatMap(_ => stream.receive(cancelScope.token))

        exchange
            .transform { result =>
                scheduled.cancel()
                result.map(interpret(table, _, logger))
            }
            .recoverWith {
                case e: CancelledException if cancelScope.token.isCancelled =>
                    Future.failed(cancelScope.token.cause.getOrElse(e))
                case e: CancelledException =>
                    Future.failed(e)
                case fde: FrameDecodeException =>
                    Future.failed(new HandshakeError.DecodeError(fde.getMessage, fde))
                case NonFatal(t) =>
                    Future.failed(t)
            }
    }

    /** Default proposal: v16..v20 with the supplied magic and `query` flag. v15 and earlier are not
      * proposed — M11 declares Conway-era as the floor and the older shapes have a different
      * version-data layout we don't model.
      */
    private def defaultProposal(magic: NetworkMagic, query: Boolean): VersionTable = {
        val versions = Seq(
          NodeToClientVersion.V16,
          NodeToClientVersion.V17,
          NodeToClientVersion.V18,
          NodeToClientVersion.V19,
          NodeToClientVersion.V20
        )
        val data = NodeToClientVersionData(magic, query)
        VersionTable(versions.map(_ -> data)*)
    }

    private def interpret(
        proposed: VersionTable,
        received: Option[HandshakeMessage],
        logger: scribe.Logger
    ): NegotiatedVersion = received match {
        case None =>
            throw new HandshakeError.DecodeError(
              "EOF before n2c handshake reply",
              cause = null
            )
        case Some(MsgAcceptVersion(version, data)) =>
            logger.info(s"n2c handshake accepted at v$version")
            NegotiatedVersion(version, data)
        case Some(MsgRefuse(reason)) =>
            logger.warn(s"n2c handshake refused: $reason")
            reason match {
                case RefuseReason.VersionMismatch(peer) =>
                    throw new HandshakeError.VersionMismatch(proposed.keySet, peer)
                case RefuseReason.HandshakeDecodeError(version, message) =>
                    throw new HandshakeError.Refused(version, s"peer decode error: $message")
                case RefuseReason.Refused(version, message) =>
                    throw new HandshakeError.Refused(version, message)
            }
        case Some(m @ (MsgQueryReply(_) | MsgProposeVersions(_))) =>
            throw new HandshakeError.UnexpectedMessage(m)
    }
}
