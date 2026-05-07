package scalus.cardano.network.jvm

import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import scalus.cardano.network.NetworkMagic
import scalus.cardano.network.infra.MiniProtocolId
import scalus.cardano.network.n2c.NodeToClientClient
import scalus.cardano.network.n2c.handshake.HandshakeError

import java.nio.file.{Files, Path}
import scala.concurrent.ExecutionContext.Implicits.global

/** Real Unix-domain-socket loopback tests for [[NodeToClientClient]]. Validates that the full stack
  * (JvmUnixDomainAsyncByteChannel → Multiplexer → N2C HandshakeDriver) works over an actual UDS —
  * these are the gate before yaci-backed N2C IT in M11.P2.
  */
class NodeToClientClientLoopbackSuite
    extends AnyFunSuite
    with ScalaFutures
    with Eventually
    with BeforeAndAfterEach {

    implicit override val patienceConfig: PatienceConfig =
        PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

    private var stub: StubN2CResponder = _
    private var socketPath: Path = _

    override def afterEach(): Unit = {
        if stub != null then stub.shutdown()
        stub = null
        if socketPath != null then {
            try Files.deleteIfExists(socketPath)
            catch { case _: Throwable => () }
            socketPath = null
        }
    }

    private def startStub(behaviour: StubN2CBehaviour = StubN2CBehaviour.AcceptV16): Path = {
        val tmpDir = Files.createTempDirectory("scalus-n2c-test")
        socketPath = tmpDir.resolve("node.sock")
        stub = new StubN2CResponder(socketPath, behaviour)
        socketPath
    }

    test("connect → handshake v16 → live connection") {
        val path = startStub()
        val conn = NodeToClientClient
            .connect(path, NetworkMagic.YaciDevnet)
            .futureValue

        try {
            assert(conn.negotiatedVersion.version == 16)
            assert(conn.negotiatedVersion.data.networkMagic.value == NetworkMagic.YaciDevnet.value)
            assert(!conn.rootToken.isCancelled)
        } finally conn.close().futureValue
    }

    test("peer refuses handshake → connect future fails with VersionMismatch") {
        val path = startStub(StubN2CBehaviour.RefuseWithVersionMismatch)
        val ex = NodeToClientClient
            .connect(path, NetworkMagic.YaciDevnet)
            .failed
            .futureValue
        assert(ex.isInstanceOf[HandshakeError.VersionMismatch], s"${ex.getClass}: ${ex.getMessage}")
    }

    test("peer sends garbage handshake reply → connect future fails with DecodeError") {
        val path = startStub(StubN2CBehaviour.MalformedHandshakeReply)
        val ex = NodeToClientClient
            .connect(path, NetworkMagic.YaciDevnet)
            .failed
            .futureValue
        assert(ex.isInstanceOf[HandshakeError.DecodeError], s"${ex.getClass}: ${ex.getMessage}")
    }

    test("close() fires rootToken and completes closed") {
        val path = startStub()
        val conn = NodeToClientClient
            .connect(path, NetworkMagic.YaciDevnet)
            .futureValue
        conn.close().futureValue
        assert(conn.rootToken.isCancelled)
        assert(conn.closed.isCompleted)
    }

    test("peer drops socket → rootToken fires") {
        val path = startStub()
        val conn = NodeToClientClient
            .connect(path, NetworkMagic.YaciDevnet)
            .futureValue
        stub.shutdown()
        eventually(assert(conn.rootToken.isCancelled))
    }

    test("channel() is idempotent across protocols") {
        val path = startStub()
        val conn = NodeToClientClient
            .connect(path, NetworkMagic.YaciDevnet)
            .futureValue
        try {
            val a = conn.channel(MiniProtocolId.LocalChainSync)
            val b = conn.channel(MiniProtocolId.LocalChainSync)
            assert(a eq b)
            val c = conn.channel(MiniProtocolId.LocalTxSubmission)
            assert(c ne a)
        } finally conn.close().futureValue
    }
}
