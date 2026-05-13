package scalus.cardano.network.n2c.localstatequery

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import scalus.cardano.infra.{CancelSource, CancelToken}
import scalus.cardano.ledger.OriginalCborByteArray
import scalus.cardano.network.infra.MiniProtocolBytes
import scalus.cardano.network.n2c.localstatequery.LocalStateQueryMessage.*
import scalus.serialization.cbor.Cbor as ScalusCbor
import scalus.uplc.builtin.ByteString

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

class LocalStateQueryDriverSuite extends AnyFunSuite with ScalaFutures {

    implicit override val patienceConfig: PatienceConfig =
        PatienceConfig(timeout = Span(2, Seconds), interval = Span(10, Millis))

    private given ExecutionContext = ExecutionContext.global

    /** Scripted bytes handle — same pattern as `LocalTxSubmissionDriverSuite.ScriptedBytes`. */
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

        def stage(reply: LocalStateQueryMessage): Unit = lock.synchronized {
            val payload = Some(ByteString.unsafeFromArray(ScalusCbor.encode(reply)))
            pending match {
                case Some(p) =>
                    pending = None
                    val _ = p.trySuccess(payload)
                case None =>
                    inbound.append(payload)
            }
        }
    }

    private def newDriver(handle: ScriptedBytes): LocalStateQueryDriver = {
        val rootScope = CancelSource()
        new LocalStateQueryDriver(handle, rootScope.token)
    }

    // ----- acquire -----

    test("acquire VolatileTip + MsgAcquired → Right(()) and isAcquired flips to true") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcquired)

        val result = driver.acquire(AcquireTarget.VolatileTip).futureValue
        assert(result == Right(()))
        assert(driver.isAcquired)
    }

    test("acquire + MsgFailure(PointTooOld) → Left and stays in Idle") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgFailure(AcquireFailure.PointTooOld))

        val result = driver.acquire(AcquireTarget.VolatileTip).futureValue
        assert(result == Left(AcquireFailure.PointTooOld))
        assert(!driver.isAcquired)
    }

    test("double acquire without release → IllegalStateException") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcquired)
        driver.acquire(AcquireTarget.VolatileTip).futureValue

        val ex = driver.acquire(AcquireTarget.VolatileTip).failed.futureValue
        assert(ex.isInstanceOf[IllegalStateException])
        assert(ex.getMessage.contains("already acquired"))
    }

    // ----- query -----

    test("query GetCurrentEra after acquire → Right(eraIndex)") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcquired)
        driver.acquire(AcquireTarget.VolatileTip).futureValue

        // Result wire bytes: bare CBOR int 6 → 0x06
        handle.stage(MsgResult(ByteString.fromArray(Array[Byte](0x06.toByte))))
        val result = driver.query(LsqQuery.GetCurrentEra).futureValue
        assert(result == Right(6))
    }

    test("query QueryIfCurrent + EraMismatch envelope → Left(LsqError.EraMismatch)") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcquired)
        driver.acquire(AcquireTarget.VolatileTip).futureValue

        // [[6, "Conway"], [5, "Babbage"]] — same shape as LsqQuerySuite uses.
        val envelope: Array[Byte] = Array(
          0x82.toByte,
          0x82.toByte,
          0x06.toByte,
          0x66.toByte
        ) ++ "Conway".getBytes("US-ASCII") ++ Array(
          0x82.toByte,
          0x05.toByte,
          0x67.toByte
        ) ++ "Babbage".getBytes("US-ASCII")
        handle.stage(MsgResult(ByteString.fromArray(envelope)))

        val q = LsqQuery.GetCurrentPParams[Unit](era = 6, decoder = _ => ())
        val result = driver.query(q).futureValue
        result match {
            case Left(LsqError.EraMismatch(expected, actual)) =>
                assert(expected == EraInfo(6, "Conway"))
                assert(actual == EraInfo(5, "Babbage"))
            case other => fail(s"expected Left(EraMismatch), got $other")
        }
    }

    test("query — inner decoder throws → Left(LsqError.DecodeFailure)") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcquired)
        driver.acquire(AcquireTarget.VolatileTip).futureValue

        val sentinel = new RuntimeException("inner-decoder-boom")
        val throwingDecoder: Array[Byte] => Unit = _ => throw sentinel
        // Envelope is valid (0x81 + inner uint 6); the inner decoder is the one that throws.
        // `0xaa` would be a CBOR map(10) header — invalid as a self-contained item — so the
        // CborMessageStream framer would hang waiting for the 20 missing key/value items.
        handle.stage(MsgResult(ByteString.fromArray(Array(0x81.toByte, 0x06.toByte))))

        val q = LsqQuery.GetCurrentPParams[Unit](era = 6, decoder = throwingDecoder)
        val result = driver.query(q).futureValue
        result match {
            case Left(LsqError.DecodeFailure(reason)) =>
                assert(reason.contains("inner-decoder-boom"))
            case other => fail(s"expected Left(DecodeFailure), got $other")
        }
    }

    test("query without acquire → IllegalStateException") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)

        val ex = driver.query(LsqQuery.GetCurrentEra).failed.futureValue
        assert(ex.isInstanceOf[IllegalStateException])
        assert(ex.getMessage.contains("no snapshot acquired"))
    }

    // ----- release -----

    test("release after acquire → isAcquired flips back; next acquire works") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcquired)
        driver.acquire(AcquireTarget.VolatileTip).futureValue
        assert(driver.isAcquired)

        driver.release().futureValue
        assert(!driver.isAcquired)

        // Second acquire should work after release.
        handle.stage(MsgAcquired)
        assert(driver.acquire(AcquireTarget.VolatileTip).futureValue == Right(()))
    }

    test("release without acquire → IllegalStateException") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        val ex = driver.release().failed.futureValue
        assert(ex.isInstanceOf[IllegalStateException])
        assert(ex.getMessage.contains("no snapshot acquired"))
    }

    // ----- close -----

    test("close on idle driver sends only MsgDone") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        driver.close().futureValue

        val sentMessages = decodeOutbound(handle.sentOutbound.toSeq)
        assert(sentMessages == Seq(MsgDone))
    }

    test("close on acquired driver sends MsgRelease then MsgDone") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcquired)
        driver.acquire(AcquireTarget.VolatileTip).futureValue
        // Drop the MsgAcquire that the test response was for.
        handle.sentOutbound.clear()

        driver.close().futureValue

        val sentMessages = decodeOutbound(handle.sentOutbound.toSeq)
        assert(sentMessages == Seq(MsgRelease, MsgDone))
    }

    test("close is idempotent") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        driver.close().futureValue
        handle.sentOutbound.clear()
        driver.close().futureValue
        assert(handle.sentOutbound.isEmpty, "second close should be a no-op")
    }

    test("acquire on closed driver → IllegalStateException") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        driver.close().futureValue

        val ex = driver.acquire(AcquireTarget.VolatileTip).failed.futureValue
        assert(ex.isInstanceOf[IllegalStateException])
        assert(ex.getMessage.contains("closed"))
    }

    // ----- helpers -----

    private def decodeOutbound(messages: Seq[ByteString]): Seq[LocalStateQueryMessage] =
        messages.map { msg =>
            val arr = msg.bytes
            given OriginalCborByteArray = OriginalCborByteArray(arr)
            io.bullet.borer.Cbor.decode(arr).to[LocalStateQueryMessage].value
        }
}
