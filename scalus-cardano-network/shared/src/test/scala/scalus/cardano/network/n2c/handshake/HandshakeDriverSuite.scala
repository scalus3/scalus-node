package scalus.cardano.network.n2c.handshake

import io.bullet.borer.Cbor
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import scalus.cardano.infra.{CancelSource, CancelToken, FakeTimer}
import scalus.cardano.network.NetworkMagic
import scalus.cardano.network.infra.MiniProtocolBytes
import scalus.cardano.network.n2c.handshake.HandshakeMessage.*
import scalus.uplc.builtin.ByteString

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}

/** Mirrors `scalus.cardano.network.handshake.HandshakeDriverSuite` for the N2C driver. */
class HandshakeDriverSuite extends AnyFunSuite with ScalaFutures {

    implicit override val patienceConfig: PatienceConfig =
        PatienceConfig(timeout = Span(2, Seconds), interval = Span(10, Millis))

    private final class ScriptedBytes extends MiniProtocolBytes {
        private val lock = new AnyRef
        private val inbound = mutable.ArrayDeque.empty[Option[ByteString]]
        private var pending: Option[Promise[Option[ByteString]]] = None
        val sentOutbound = mutable.ArrayBuffer.empty[ByteString]

        def receive(cancel: CancelToken = CancelToken.never): Future[Option[ByteString]] =
            lock.synchronized {
                if cancel.isCancelled then
                    Future.failed(cancel.cause.getOrElse(new RuntimeException("cancelled")))
                else if inbound.nonEmpty then Future.successful(inbound.removeHead())
                else {
                    val p = Promise[Option[ByteString]]()
                    pending = Some(p)
                    cancel.onCancel { () =>
                        lock.synchronized {
                            if pending.exists(_ eq p) then {
                                pending = None
                                val _ = p.tryFailure(
                                  cancel.cause.getOrElse(new RuntimeException("cancelled"))
                                )
                            }
                        }
                    }
                    p.future
                }
            }

        def send(message: ByteString, cancel: CancelToken = CancelToken.never): Future[Unit] = {
            sentOutbound += message
            Future.unit
        }

        def stage(reply: HandshakeMessage): Unit =
            offer(Some(ByteString.unsafeFromArray(Cbor.encode(reply).toByteArray)))

        def stageRawBytes(bs: ByteString): Unit = offer(Some(bs))

        def stageEof(): Unit = offer(None)

        private def offer(value: Option[ByteString]): Unit = lock.synchronized {
            pending match {
                case Some(p) =>
                    pending = None
                    val _ = p.trySuccess(value)
                case None => inbound.append(value)
            }
        }
    }

    private def newFixture(): (ScriptedBytes, CancelSource, FakeTimer) = {
        val timer = new FakeTimer()
        val scope = CancelSource()
        (new ScriptedBytes, scope, timer)
    }

    private val magic = NetworkMagic.Mainnet
    private val expectedProposed: Set[Int] = Set(16, 17, 18, 19, 20)

    test("driver sends MsgProposeVersions(v16..v20) and returns on MsgAcceptVersion") {
        val (peer, scope, timer) = newFixture()
        val f = HandshakeDriver.run(peer, magic, scope, timer)

        val acceptedData = NodeToClientVersionData(magic, query = false)
        peer.stage(MsgAcceptVersion(16, acceptedData))

        val negotiated = f.futureValue
        assert(negotiated == NegotiatedVersion(16, acceptedData))

        assert(peer.sentOutbound.size == 1)
        Cbor.decode(peer.sentOutbound.head.bytes).to[HandshakeMessage].value match {
            case MsgProposeVersions(table) =>
                assert(table.keySet == expectedProposed)
                assert(table(16).networkMagic.value == magic.value)
                assert(table(16).query == false)
            case other => fail(s"expected MsgProposeVersions, got $other")
        }
    }

    test("query=true is propagated into the proposed version-data") {
        val (peer, scope, timer) = newFixture()
        val f = HandshakeDriver.run(peer, magic, scope, timer, query = true)
        peer.stage(
          MsgAcceptVersion(16, NodeToClientVersionData(magic, query = true))
        )
        val _ = f.futureValue

        val proposal = Cbor.decode(peer.sentOutbound.head.bytes).to[HandshakeMessage].value
        proposal.asInstanceOf[MsgProposeVersions].table.values.foreach { d =>
            assert(d.query == true)
        }
    }

    test("MsgRefuse/VersionMismatch surfaces as HandshakeError.VersionMismatch") {
        val (peer, scope, timer) = newFixture()
        val f = HandshakeDriver.run(peer, magic, scope, timer)
        peer.stage(MsgRefuse(RefuseReason.VersionMismatch(List(14, 15))))

        val ex = f.failed.futureValue
        assert(ex.isInstanceOf[HandshakeError.VersionMismatch])
        val vm = ex.asInstanceOf[HandshakeError.VersionMismatch]
        assert(vm.proposed == expectedProposed)
        assert(vm.peerSupported == List(14, 15))
    }

    test("MsgRefuse/Refused surfaces as HandshakeError.Refused") {
        val (peer, scope, timer) = newFixture()
        val f = HandshakeDriver.run(peer, magic, scope, timer)
        peer.stage(MsgRefuse(RefuseReason.Refused(16, "magic mismatch")))

        val ex = f.failed.futureValue
        assert(ex.isInstanceOf[HandshakeError.Refused])
        val r = ex.asInstanceOf[HandshakeError.Refused]
        assert(r.versionTried == 16)
        assert(r.reason == "magic mismatch")
    }

    test("MsgRefuse/HandshakeDecodeError maps to Refused with 'peer decode error' prefix") {
        val (peer, scope, timer) = newFixture()
        val f = HandshakeDriver.run(peer, magic, scope, timer)
        peer.stage(MsgRefuse(RefuseReason.HandshakeDecodeError(16, "bad cbor")))

        val ex = f.failed.futureValue.asInstanceOf[HandshakeError.Refused]
        assert(ex.versionTried == 16)
        assert(ex.reason.contains("bad cbor"))
    }

    test("MsgQueryReply in initiator flow surfaces UnexpectedMessage") {
        val (peer, scope, timer) = newFixture()
        val f = HandshakeDriver.run(peer, magic, scope, timer)
        val reply = MsgQueryReply(
          VersionTable(NodeToClientVersion.V16 -> NodeToClientVersionData(magic, query = false))
        )
        peer.stage(reply)

        val ex = f.failed.futureValue.asInstanceOf[HandshakeError.UnexpectedMessage]
        assert(ex.received == reply)
    }

    test("EOF before reply surfaces as DecodeError") {
        val (peer, scope, timer) = newFixture()
        val f = HandshakeDriver.run(peer, magic, scope, timer)
        peer.stageEof()
        val ex = f.failed.futureValue
        assert(ex.isInstanceOf[HandshakeError.DecodeError])
    }

    test("garbage bytes surface as DecodeError") {
        val (peer, scope, timer) = newFixture()
        val f = HandshakeDriver.run(peer, magic, scope, timer)
        peer.stageRawBytes(ByteString.unsafeFromArray(Array[Byte](0xff.toByte, 0x00.toByte)))
        val ex = f.failed.futureValue
        assert(ex.isInstanceOf[HandshakeError.DecodeError], s"got ${ex.getClass}: ${ex.getMessage}")
    }

    test("timeout fires HandshakeError.Timeout and marks cancelScope") {
        val timer = new FakeTimer()
        val scope = CancelSource()
        val peer = new ScriptedBytes
        val f = HandshakeDriver.run(peer, magic, scope, timer, timeout = 30.seconds)

        timer.advance(29.seconds)
        assert(!f.isCompleted)
        timer.advance(2.seconds)

        val ex = f.failed.futureValue
        assert(ex.isInstanceOf[HandshakeError.Timeout])
        assert(scope.token.isCancelled)
        assert(scope.token.cause.exists(_.isInstanceOf[HandshakeError.Timeout]))
    }

    test("early success cancels the scheduled timeout") {
        val timer = new FakeTimer()
        val scope = CancelSource()
        val peer = new ScriptedBytes
        val f = HandshakeDriver.run(peer, magic, scope, timer)

        peer.stage(MsgAcceptVersion(16, NodeToClientVersionData(magic, query = false)))
        val _ = f.futureValue
        assert(timer.pendingCount == 0)
    }

    test("outer scope cancel before reply propagates the outer cause") {
        val (peer, scope, timer) = newFixture()
        val f = HandshakeDriver.run(peer, magic, scope, timer)

        val outerCause = new RuntimeException("outer cancel")
        scope.cancel(outerCause)
        val ex = f.failed.futureValue
        assert(ex eq outerCause, s"expected outer cause, got $ex")
    }
}
