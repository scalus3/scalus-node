package scalus.cardano.network.n2c.localtxsubmission

import io.bullet.borer.Cbor
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import scalus.cardano.infra.{CancelSource, CancelToken}
import scalus.cardano.network.infra.MiniProtocolBytes
import scalus.cardano.network.n2c.localtxsubmission.LocalTxSubmissionMessage.*
import scalus.uplc.builtin.ByteString

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

class LocalTxSubmissionDriverSuite extends AnyFunSuite with ScalaFutures {

    implicit override val patienceConfig: PatienceConfig =
        PatienceConfig(timeout = Span(2, Seconds), interval = Span(10, Millis))

    private given ExecutionContext = ExecutionContext.global

    /** Scripted bytes handle for the driver — mirrors `KeepAliveDriverSuite.ScriptedBytes`. Tests
      * stage replies via `stage(...)` and read what the driver wrote via `sentOutbound`.
      */
    private final class ScriptedBytes extends MiniProtocolBytes {
        private val lock = new AnyRef
        private val inbound = mutable.ArrayDeque.empty[Option[ByteString]]
        private var pending: Option[Promise[Option[ByteString]]] = None
        val sentOutbound: mutable.ArrayBuffer[ByteString] = mutable.ArrayBuffer.empty

        def receive(cancel: CancelToken = CancelToken.never): Future[Option[ByteString]] =
            lock.synchronized {
                if inbound.nonEmpty then Future.successful(inbound.removeHead())
                else {
                    val p = Promise[Option[ByteString]]()
                    pending = Some(p)
                    p.future
                }
            }

        def send(message: ByteString, cancel: CancelToken = CancelToken.never): Future[Unit] =
            lock.synchronized {
                sentOutbound += message
                Future.unit
            }

        def stage(reply: LocalTxSubmissionMessage): Unit = lock.synchronized {
            val payload = Some(ByteString.unsafeFromArray(Cbor.encode(reply).toByteArray))
            pending match {
                case Some(p) =>
                    pending = None
                    val _ = p.trySuccess(payload)
                case None =>
                    inbound.append(payload)
            }
        }
    }

    private def newDriver(handle: ScriptedBytes): LocalTxSubmissionDriver = {
        val rootScope = CancelSource()
        new LocalTxSubmissionDriver(handle, rootScope.token)
    }

    test("submit + MsgAcceptTx returns Right(())") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcceptTx)

        val result = driver.submit(era = 6, ByteString.fromArray(Array[Byte](0xa0.toByte)))
        assert(result.futureValue.isRight)
        assert(handle.sentOutbound.size == 1, "exactly one MsgSubmitTx should have been sent")
    }

    test("submit + MsgRejectTx returns Left(LocalTxSubmissionRejection)") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        val reason = ByteString.fromArray(Array[Byte](0x82.toByte, 0x01, 0x02))
        handle.stage(MsgRejectTx(era = 6, reason))

        val result = driver.submit(era = 6, ByteString.fromArray(Array[Byte](0xa0.toByte)))
        result.futureValue match
            case Left(LocalTxSubmissionRejection(rejectEra, reasonBytes)) =>
                assert(rejectEra == 6)
                assert(reasonBytes == reason)
            case Right(_) => fail("expected reject, got accept")
    }

    test("peer-sent MsgDone mid-submit is a protocol violation") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgDone)
        val cause =
            driver
                .submit(era = 6, ByteString.fromArray(Array[Byte](0xa0.toByte)))
                .failed
                .futureValue
        assert(cause.isInstanceOf[IllegalStateException], s"got: $cause")
    }

    test("close after submit accept allows close to be invoked idempotently") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcceptTx)
        driver.submit(era = 6, ByteString.fromArray(Array[Byte](0xa0.toByte))).futureValue

        driver.close().futureValue
        val cause =
            driver
                .submit(era = 6, ByteString.fromArray(Array[Byte](0xa0.toByte)))
                .failed
                .futureValue
        assert(cause.isInstanceOf[IllegalStateException], s"got: $cause")
        driver.close().futureValue
    }
}
