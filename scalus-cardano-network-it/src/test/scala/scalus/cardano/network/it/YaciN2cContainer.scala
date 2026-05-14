package scalus.cardano.network.it

import com.bloxbean.cardano.yaci.test.YaciCardanoContainer

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

    lazy val container: YaciCardanoContainer = {
        val c = new YaciCardanoContainer()
        // Enable the socat n2c bridge — gated on `is.docker` in yaci-cli's SocatService, which the
        // bare image leaves false. We *are* in docker here, so this is just correcting the image.
        c.addEnv("IS_DOCKER", "true")
        c.addExposedPort(SocatN2cPort)
        c.start()
        c
    }
}

/** Mix-in giving N2C suites the host/port of the socat n2c bridge. */
trait YaciN2cAccess {

    protected def n2cHost: String = YaciN2cContainer.container.getHost

    protected def n2cPort: Int =
        YaciN2cContainer.container.getMappedPort(YaciN2cContainer.SocatN2cPort)
}
