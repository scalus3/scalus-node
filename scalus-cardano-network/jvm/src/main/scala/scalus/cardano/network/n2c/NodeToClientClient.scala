package scalus.cardano.network.n2c

import scalus.cardano.infra.jvm.JvmTimer
import scalus.cardano.infra.{CancelSource, CancelToken, CancelledException, Timer}
import scalus.cardano.network.NetworkMagic
import scalus.cardano.network.infra.*
import scalus.cardano.network.jvm.{JvmAsyncByteChannel, JvmUnixDomainAsyncByteChannel}
import scalus.cardano.network.n2c.handshake.{HandshakeDriver, NegotiatedVersion}

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

/** JVM factory for [[NodeToClientConnection]]. Opens a transport to a cardano-node — a Unix-domain
  * socket ([[connect]]) or a TCP endpoint ([[connectTcp]]) — runs the N2C handshake, and returns
  * the live connection.
  *
  * Mirrors [[scalus.cardano.network.NodeToNodeClient]] except that:
  *   - the default transport is a Unix-domain socket (JDK 16+);
  *   - there is no keep-alive loop — N2C has no keep-alive mini-protocol;
  *   - the version table is the N2C one (`>= V16`, `[networkMagic, query]` per-version data).
  *
  * The TCP variant exists because some deployments front the node socket with a `socat`
  * `TCP-LISTEN … UNIX-CONNECT` bridge — yaci-devkit does this by default on port 3333, which is how
  * the Yaci LSQ/LTM integration suites reach the node. The N2C mini-protocols are
  * transport-agnostic (they run over [[AsyncByteChannel]]), so the bridge is wire-identical to a
  * direct socket connection.
  *
  * See `docs/local/design/n2c-protocol-m11.md` for the milestone scope.
  */
object NodeToClientClient {

    private val defaultLogger: scribe.Logger =
        scribe.Logger("scalus.cardano.network.n2c.NodeToClientClient")

    /** Establish a Node-to-Client connection over a Unix-domain socket. The returned future
      * completes once the handshake is accepted. Any failure before that point (socket error,
      * handshake refusal, handshake timeout) fails the returned future and tears down the transient
      * socket.
      */
    def connect(
        socketPath: Path,
        networkMagic: NetworkMagic,
        config: ClientConfig = ClientConfig.default,
        timer: Timer = JvmTimer.shared,
        logger: scribe.Logger = defaultLogger
    )(using ExecutionContext): Future[NodeToClientConnection] =
        connectVia(
          JvmUnixDomainAsyncByteChannel.connect(socketPath),
          networkMagic,
          config,
          timer,
          logger
        )

    /** Establish a Node-to-Client connection over TCP — e.g. to a `socat` bridge fronting the node
      * socket. Wire-identical to [[connect]] once the channel is open; only the transport differs.
      */
    def connectTcp(
        host: String,
        port: Int,
        networkMagic: NetworkMagic,
        config: ClientConfig = ClientConfig.default,
        timer: Timer = JvmTimer.shared,
        logger: scribe.Logger = defaultLogger
    )(using ExecutionContext): Future[NodeToClientConnection] =
        connectVia(JvmAsyncByteChannel.connect(host, port), networkMagic, config, timer, logger)

    private def connectVia(
        openChannel: Future[AsyncByteChannel],
        networkMagic: NetworkMagic,
        config: ClientConfig,
        timer: Timer,
        logger: scribe.Logger
    )(using ExecutionContext): Future[NodeToClientConnection] = {
        for
            channel <- openChannel
            connection <- buildConnection(channel, networkMagic, config, timer, logger)
                .recoverWith { case NonFatal(t) =>
                    channel.close().transformWith(_ => Future.failed(t))
                }
        yield connection
    }

    private def buildConnection(
        channel: AsyncByteChannel,
        networkMagic: NetworkMagic,
        config: ClientConfig,
        timer: Timer,
        logger: scribe.Logger
    )(using ExecutionContext): Future[NodeToClientConnection] = {
        val connectionRoot = CancelSource()
        val mux = new Multiplexer(
          channel,
          connectionRoot,
          MuxConfig(
            sduMaxPayload = config.sduMaxPayload,
            mailboxCapacity = config.mailboxCapacity
          ),
          logger = logger
        )

        mux.closed.onComplete {
            case scala.util.Success(_) =>
                if !connectionRoot.token.isCancelled then
                    connectionRoot.cancel(new AsyncByteChannel.ChannelClosedException("peer EOF"))
            case scala.util.Failure(t) =>
                if !connectionRoot.token.isCancelled then connectionRoot.cancel(t)
        }

        val _ = connectionRoot.token.onCancel { () =>
            val _ = channel.close()
        }

        val handshakeScope = CancelSource.linkedTo(connectionRoot.token)
        val handshakeHandle = mux.channel(MiniProtocolId.Handshake)

        val handshakeFuture = HandshakeDriver.run(
          handshakeHandle,
          networkMagic,
          handshakeScope,
          timer,
          timeout = config.handshakeTimeout,
          query = config.query,
          logger = logger
        )

        handshakeFuture.map { negotiated =>
            logger.info(s"n2c handshake complete, negotiated v${negotiated.version}")
            buildConnectionObject(mux, connectionRoot, negotiated)
        }
    }

    private def buildConnectionObject(
        mux: Multiplexer,
        connectionRoot: CancelSource,
        negotiated: NegotiatedVersion
    )(using ExecutionContext): NodeToClientConnection = {
        val closedPromise = Promise[Unit]()
        val _ = connectionRoot.token.onCancel { () =>
            val cause = connectionRoot.token.cause
                .getOrElse(new CancelledException("connection closed"))
            cause match {
                case _: AsyncByteChannel.ChannelClosedException =>
                    val _ = closedPromise.trySuccess(())
                case other =>
                    val _ = closedPromise.tryFailure(other)
            }
        }

        new NodeToClientConnection {
            def negotiatedVersion: NegotiatedVersion = negotiated
            def channel(proto: MiniProtocolId): MiniProtocolBytes = mux.channel(proto)
            def rootToken: CancelToken = connectionRoot.token
            def closed: Future[Unit] = closedPromise.future
            def close(): Future[Unit] = {
                if !connectionRoot.token.isCancelled then
                    connectionRoot.cancel(new AsyncByteChannel.ChannelClosedException("user close"))
                closedPromise.future.recover { case _ => () }
            }
        }
    }
}
