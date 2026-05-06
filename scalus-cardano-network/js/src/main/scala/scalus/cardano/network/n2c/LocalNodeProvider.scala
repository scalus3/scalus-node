package scalus.cardano.network.n2c

import scalus.cardano.ledger.CardanoInfo
import scalus.cardano.network.NetworkMagic
import scalus.cardano.node.BlockchainProvider

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}

/** Scala.js stub for [[LocalNodeProvider]]. Scala.js has no Unix-domain socket primitive (and the
  * underlying [[NodeToClientClient]] is similarly stubbed), so any `connect` attempt fails eagerly
  * with [[UnsupportedOperationException]] — mirrors the existing JS stubs for
  * [[NodeToClientClient]] / `NodeToNodeClient`.
  *
  * Keeping the FQN aligned with the JVM impl lets cross-built callers (the streaming providers'
  * `buildBackup` arm for `BackupSource.LocalNode`) reference `LocalNodeProvider.connect` uniformly;
  * the JS build compiles cleanly, and a JS caller that actually picks `BackupSource.LocalNode` gets
  * a descriptive runtime failure.
  */
object LocalNodeProvider {

    def connect(
        socketPath: Path,
        networkMagic: NetworkMagic,
        cardanoInfo: CardanoInfo,
        submitEra: Int = 6
    )(using ExecutionContext): Future[BlockchainProvider] =
        Future.failed(
          new UnsupportedOperationException(
            s"LocalNodeProvider.connect($socketPath, magic=$networkMagic) is not available on " +
                s"Scala.js — Unix-domain sockets are JVM-only. Run on JVM, or pick a different " +
                s"BackupSource."
          )
        )
}
