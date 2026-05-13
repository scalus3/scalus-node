package scalus.cardano.network.n2c

import scalus.cardano.ledger.CardanoInfo
import scalus.cardano.network.NetworkMagic

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}

/** Scala.js stub for [[LocalNodeAccess]]. Scala.js has no Unix-domain socket primitive (and the
  * underlying [[NodeToClientClient]] is similarly stubbed), so any `connect` attempt fails eagerly
  * with [[UnsupportedOperationException]] — mirrors the existing JS stubs for
  * [[NodeToClientClient]] / `NodeToNodeClient`.
  *
  * The JVM impl is not source-compatible (its return type is the real `LocalNodeAccess`), so the JS
  * stub only exposes a no-op `connect`. Cross-built callers that need to reach a local node use the
  * `scalus-local-node-backup` module's composite, which has its own JS stub.
  */
object LocalNodeAccess {

    def connect(
        socketPath: Path,
        networkMagic: NetworkMagic,
        cardanoInfo: CardanoInfo,
        submitEra: Int = 6
    )(using ExecutionContext): Future[Nothing] =
        Future.failed(
          new UnsupportedOperationException(
            s"LocalNodeAccess.connect($socketPath, magic=$networkMagic) is not available on " +
                s"Scala.js — Unix-domain sockets are JVM-only. Run on JVM, or pick a different " +
                s"BackupSource."
          )
        )
}
