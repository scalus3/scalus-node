package scalus.cardano.network.jvm

import io.bullet.borer.Cbor
import scalus.cardano.network.NetworkMagic
import scalus.cardano.network.infra.{Direction, MiniProtocolId, Sdu}
import scalus.cardano.network.n2c.handshake.HandshakeMessage.*
import scalus.cardano.network.n2c.handshake.{HandshakeMessage, NodeToClientVersion, NodeToClientVersionData, RefuseReason}

import java.io.{EOFException, InputStream, OutputStream}
import java.net.{StandardProtocolFamily, UnixDomainSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ServerSocketChannel, SocketChannel}
import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Unix-domain-socket loopback peer for [[scalus.cardano.network.n2c.NodeToClientClient]]. Mirrors
  * [[StubN2NResponder]] but on a Unix-domain socket — the JDK's `AsynchronousSocketChannel` family
  * does not support `StandardProtocolFamily.UNIX`, so this stub uses blocking [[SocketChannel]]
  * accept-loop with one handler thread per accepted connection.
  */
final class StubN2CResponder(
    socketPath: Path,
    behaviour: StubN2CBehaviour = StubN2CBehaviour.AcceptV16
) {
    Files.deleteIfExists(socketPath)
    private val server: ServerSocketChannel =
        ServerSocketChannel.open(StandardProtocolFamily.UNIX)
    server.bind(UnixDomainSocketAddress.of(socketPath))

    private val running = new AtomicBoolean(true)
    private val connectedCount = new AtomicInteger(0)
    private val openSockets = ConcurrentHashMap.newKeySet[SocketChannel]()
    private val thread: Thread = new Thread(() => acceptLoop(), "scalus-stub-n2c-responder")
    thread.setDaemon(true)
    thread.start()

    def path: Path = socketPath

    def connectionsAccepted: Int = connectedCount.get

    def shutdown(): Unit = {
        running.set(false)
        try server.close()
        catch { case NonFatal(_) => () }
        openSockets.asScala.foreach { s =>
            try s.close()
            catch { case NonFatal(_) => () }
        }
        openSockets.clear()
        try Files.deleteIfExists(socketPath)
        catch { case NonFatal(_) => () }
    }

    private def acceptLoop(): Unit = {
        while running.get && server.isOpen do {
            try {
                val sc = server.accept()
                connectedCount.incrementAndGet()
                val handler = new Thread(
                  () => handleConnection(sc),
                  s"stub-n2c-handler-$connectedCount"
                )
                handler.setDaemon(true)
                handler.start()
            } catch {
                case _: java.nio.channels.ClosedChannelException if !running.get     => ()
                case _: java.nio.channels.AsynchronousCloseException if !running.get => ()
                case NonFatal(t) =>
                    if running.get then System.err.println(s"stub n2c accept error: $t")
            }
        }
    }

    private def handleConnection(sc: SocketChannel): Unit = {
        openSockets.add(sc)
        try behaviour.run(sc)
        catch { case NonFatal(_) => () }
        finally {
            openSockets.remove(sc)
            try sc.close()
            catch { case NonFatal(_) => () }
        }
    }
}

trait StubN2CBehaviour {
    def run(sc: SocketChannel): Unit
}

object StubN2CBehaviour {

    /** Accept the handshake at v16 with the YaciDevnet magic and `query=false`. After accepting,
      * idle until the peer disconnects.
      */
    case object AcceptV16 extends StubN2CBehaviour {
        def run(sc: SocketChannel): Unit = StubN2CWire.runAcceptingPeer(
          sc,
          accept = true,
          refuseReason = None
        )
    }

    /** Refuse with `MsgRefuse(VersionMismatch(List(99)))`. */
    case object RefuseWithVersionMismatch extends StubN2CBehaviour {
        def run(sc: SocketChannel): Unit = StubN2CWire.runAcceptingPeer(
          sc,
          accept = false,
          refuseReason = Some(RefuseReason.VersionMismatch(List(99)))
        )
    }

    /** Send garbage CBOR for the handshake reply. */
    case object MalformedHandshakeReply extends StubN2CBehaviour {
        def run(sc: SocketChannel): Unit = {
            val _ = StubN2CWire.readOneSdu(sc)
            val garbage = Array[Byte](0xff.toByte, 0xff.toByte, 0xff.toByte)
            StubN2CWire.writeSdu(sc, MiniProtocolId.Handshake, Direction.Responder, garbage)
        }
    }
}

private object StubN2CWire {

    final case class RawSdu(proto: MiniProtocolId, direction: Direction, payload: Array[Byte])

    def readOneSdu(sc: SocketChannel): RawSdu = {
        val in: InputStream = Channels.newInputStream(sc)
        val header = readFully(in, Sdu.HeaderSize)
        val parsed = Sdu.parseHeader(header)
        val proto = parsed.protocol.getOrElse(
          throw new IllegalStateException(s"unknown protocol wire=${parsed.protocolWire}")
        )
        val payload = readFully(in, parsed.length)
        RawSdu(proto, parsed.direction, payload)
    }

    def writeSdu(
        sc: SocketChannel,
        proto: MiniProtocolId,
        direction: Direction,
        payload: Array[Byte]
    ): Unit = {
        val header = Sdu.encodeHeader(
          timestamp = 0,
          protocol = proto,
          direction = direction,
          length = payload.length
        )
        val out: OutputStream = Channels.newOutputStream(sc)
        out.write(header)
        out.write(payload)
        out.flush()
    }

    def runAcceptingPeer(
        sc: SocketChannel,
        accept: Boolean,
        refuseReason: Option[RefuseReason]
    ): Unit = {
        val proposeFrame = readOneSdu(sc)
        require(
          proposeFrame.proto == MiniProtocolId.Handshake,
          s"first frame not handshake: $proposeFrame"
        )
        val propose = Cbor.decode(proposeFrame.payload).to[HandshakeMessage].value
        propose match {
            case MsgProposeVersions(_) => ()
            case other =>
                throw new IllegalStateException(s"expected MsgProposeVersions, got $other")
        }

        val reply: HandshakeMessage =
            if accept then
                // Mirror real cardano-node behaviour: on a testnet (YaciDevnet here) the version
                // is echoed back with bit 15 set. Driver strips this when populating
                // NegotiatedVersion so consumers see the logical V16 number.
                MsgAcceptVersion(
                  NodeToClientVersion.V16 | 0x8000,
                  NodeToClientVersionData(NetworkMagic.YaciDevnet, query = false)
                )
            else MsgRefuse(refuseReason.getOrElse(RefuseReason.VersionMismatch(Nil)))

        writeSdu(
          sc,
          MiniProtocolId.Handshake,
          Direction.Responder,
          Cbor.encode(reply).toByteArray
        )

        if !accept then return

        // Idle: just wait for the peer to disconnect by reading until EOF.
        val in = Channels.newInputStream(sc)
        try {
            val buf = new Array[Byte](256)
            while in.read(buf) >= 0 do ()
        } catch {
            case _: EOFException | _: java.nio.channels.ClosedChannelException => ()
        }
    }

    private def readFully(in: InputStream, n: Int): Array[Byte] = {
        val buf = new Array[Byte](n)
        var off = 0
        while off < n do {
            val r = in.read(buf, off, n - off)
            if r < 0 then throw new EOFException(s"EOF after $off/$n bytes")
            off += r
        }
        buf
    }

    // Suppress unused-import warning for ByteBuffer (kept for parity with NIO style)
    @SuppressWarnings(Array("unused")) private val _bb = classOf[ByteBuffer]
}
