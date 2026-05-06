package scalus.cardano.network.n2c

import scalus.cardano.infra.Timer
import scalus.cardano.network.NetworkMagic

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}

/** Scala.js stub for [[NodeToClientClient]]. Scala.js has no Unix-domain socket primitive, so the
  * connect path fails eagerly with [[UnsupportedOperationException]] — same shape as the N2N JS
  * stub.
  *
  * Keeping the object FQN identical to the JVM version lets cross-built callers (the streaming
  * providers' N2C arms) reference `NodeToClientClient.connect` uniformly; the JS build compiles
  * cleanly, and a JS caller that actually picks `ChainSyncSource.N2C` gets a descriptive runtime
  * failure.
  */
object NodeToClientClient {

    private val defaultLogger: scribe.Logger =
        scribe.Logger("scalus.cardano.network.n2c.NodeToClientClient")

    def connect(
        socketPath: Path,
        networkMagic: NetworkMagic,
        config: ClientConfig = ClientConfig.default,
        timer: Timer = null,
        logger: scribe.Logger = defaultLogger
    )(using ExecutionContext): Future[NodeToClientConnection] =
        Future.failed(
          new UnsupportedOperationException(
            s"NodeToClientClient.connect($socketPath, magic=$networkMagic) is not available on " +
                s"Scala.js — Unix-domain sockets are JVM-only. Run on JVM, or pick a different " +
                s"ChainSyncSource."
          )
        )
}
