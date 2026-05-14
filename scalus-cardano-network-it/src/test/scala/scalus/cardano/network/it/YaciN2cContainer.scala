package scalus.cardano.network.it

import com.bloxbean.cardano.yaci.test.{Funding, YaciCardanoContainer}
import scalus.cardano.ledger.CardanoInfo
import scalus.cardano.network.NetworkMagic
import scalus.cardano.network.n2c.LocalNodeAccess

import scala.annotation.tailrec
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.*

/** Shared `YaciCardanoContainer` for the N2C integration suites, configured to expose the node's
  * Node-to-Client surface over TCP.
  *
  * The stock `bloxbean/yaci-cli` image runs the cardano-node N2C socket as a Unix socket inside the
  * container, and `YaciCardanoContainer` (from `yaci-cardano-test`) doesn't surface it. yaci-cli
  * *does* ship a `socat TCP-LISTEN:3333 … UNIX-CONNECT:node/node.sock` bridge — but it only starts
  * when the `is.docker` Spring property is true, and the bare image doesn't bake that in (only the
  * full devkit's `config/env` sets `IS_DOCKER=true`). So we set it ourselves and add `3333` to the
  * exposed-port list.
  *
  * Lifecycle mirrors `scalus.testing.yaci.YaciContainer`: started lazily on first access, never
  * explicitly stopped — testcontainers' ryuk reaps it. One container is shared by every suite that
  * mixes in [[YaciN2cAccess]].
  */
object YaciN2cContainer {

    /** In-container TCP port of the socat → node.sock bridge. yaci-cli's default
      * `ClusterInfo.socatPort`.
      */
    val SocatN2cPort: Int = 3333

    /** Testnet address funded at devnet startup. The funding tx lands in the first block(s) and is
      * never spent, so it's a stable fixture both backends should agree on — used by
      * [[YaciCrossBackupParitySuite]]. `withInitialFunding` also strengthens the container's
      * readiness gate: it additionally waits for this address's UTxOs to be queryable.
      */
    val FundedAddress: String =
        "addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp"

    /** Ada topped up to [[FundedAddress]] at startup. */
    val FundedAda: Long = 5000L

    lazy val container: YaciCardanoContainer = {
        val c = new YaciCardanoContainer()
        // Enable the socat n2c bridge — gated on `is.docker` in yaci-cli's SocatService, which the
        // bare image leaves false. We *are* in docker here, so this is just correcting the image.
        c.addEnv("IS_DOCKER", "true")
        c.addExposedPort(SocatN2cPort)
        c.withInitialFunding(new Funding(FundedAddress, FundedAda))
        c.start()
        c
    }
}

/** Mix-in giving N2C suites the host/port of the socat n2c bridge and a connect helper. */
trait YaciN2cAccess {

    protected def n2cHost: String = YaciN2cContainer.container.getHost

    protected def n2cPort: Int =
        YaciN2cContainer.container.getMappedPort(YaciN2cContainer.SocatN2cPort)

    /** Open a [[LocalNodeAccess]] over the socat bridge, retrying briefly: the bridge can lag the
      * container's HTTP wait strategy by a beat (both fire off the same `ClusterStarted` event,
      * order unspecified).
      */
    protected def connectLocalNode()(using ExecutionContext): LocalNodeAccess = {
        @tailrec
        def go(attemptsLeft: Int): LocalNodeAccess =
            try
                Await.result(
                  LocalNodeAccess
                      .connectTcp(n2cHost, n2cPort, NetworkMagic.YaciDevnet, CardanoInfo.preview),
                  30.seconds
                )
            catch {
                case _: Throwable if attemptsLeft > 1 =>
                    Thread.sleep(500)
                    go(attemptsLeft - 1)
            }
        go(attemptsLeft = 10)
    }
}
