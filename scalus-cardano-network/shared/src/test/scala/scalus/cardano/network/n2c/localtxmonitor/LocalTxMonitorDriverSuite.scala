package scalus.cardano.network.n2c.localtxmonitor

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import scalus.cardano.infra.CancelSource
import scalus.cardano.ledger.{OriginalCborByteArray, TransactionHash}
import scalus.cardano.network.infra.ScriptedMiniProtocolBytes
import scalus.cardano.network.n2c.localtxmonitor.LocalTxMonitorMessage.*
import scalus.uplc.builtin.ByteString

import scala.concurrent.ExecutionContext

class LocalTxMonitorDriverSuite extends AnyFunSuite with ScalaFutures {

    implicit override val patienceConfig: PatienceConfig =
        PatienceConfig(timeout = Span(2, Seconds), interval = Span(10, Millis))

    private given ExecutionContext = ExecutionContext.global

    private type ScriptedBytes = ScriptedMiniProtocolBytes[LocalTxMonitorMessage]

    private def newDriver(handle: ScriptedBytes): LocalTxMonitorDriver = {
        val rootScope = CancelSource()
        new LocalTxMonitorDriver(handle, rootScope.token)
    }

    private val anyHash: TransactionHash =
        TransactionHash.fromByteString(ByteString.fromArray(Array.fill[Byte](32)(0x11)))

    // ----- acquire -----

    test("acquire + MsgAcquired(slot) → returns slot and isAcquired flips to true") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcquired(slot = 12345L))

        val slot = driver.acquire().futureValue
        assert(slot == 12345L)
        assert(driver.isAcquired)
    }

    test("acquire absorbs interleaved MsgAwaitAcquire until MsgAcquired lands") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAwaitAcquire)
        handle.stage(MsgAwaitAcquire)
        handle.stage(MsgAcquired(slot = 7L))

        val slot = driver.acquire().futureValue
        assert(slot == 7L)
        assert(driver.isAcquired)
    }

    test("double acquire without release → IllegalStateException") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcquired(slot = 1L))
        driver.acquire().futureValue

        val ex = driver.acquire().failed.futureValue
        assert(ex.isInstanceOf[IllegalStateException])
        assert(ex.getMessage.contains("already acquired"))
    }

    // ----- hasTx -----

    test("hasTx after acquire + MsgRespondHasTx(true) → true") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcquired(slot = 1L))
        driver.acquire().futureValue

        handle.stage(MsgRespondHasTx(true))
        assert(driver.hasTx(era = 6, anyHash).futureValue)
    }

    test("hasTx after acquire + MsgRespondHasTx(false) → false") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcquired(slot = 1L))
        driver.acquire().futureValue

        handle.stage(MsgRespondHasTx(false))
        assert(!driver.hasTx(era = 6, anyHash).futureValue)
    }

    test("hasTx without acquire → IllegalStateException") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)

        val ex = driver.hasTx(era = 6, anyHash).failed.futureValue
        assert(ex.isInstanceOf[IllegalStateException])
        assert(ex.getMessage.contains("no snapshot acquired"))
    }

    // ----- release -----

    test("release after acquire → isAcquired flips back; next acquire works") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcquired(slot = 1L))
        driver.acquire().futureValue
        assert(driver.isAcquired)

        driver.release().futureValue
        assert(!driver.isAcquired)

        handle.stage(MsgAcquired(slot = 2L))
        assert(driver.acquire().futureValue == 2L)
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

        assert(decodeOutbound(handle.sentOutbound.toSeq) == Seq(MsgDone))
    }

    test("close on acquired driver sends MsgRelease then MsgDone") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        handle.stage(MsgAcquired(slot = 1L))
        driver.acquire().futureValue
        handle.sentOutbound.clear()

        driver.close().futureValue
        assert(decodeOutbound(handle.sentOutbound.toSeq) == Seq(MsgRelease, MsgDone))
    }

    test("close is idempotent") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        driver.close().futureValue
        handle.sentOutbound.clear()
        driver.close().futureValue
        assert(handle.sentOutbound.isEmpty)
    }

    test("acquire on closed driver → IllegalStateException") {
        val handle = new ScriptedBytes
        val driver = newDriver(handle)
        driver.close().futureValue

        val ex = driver.acquire().failed.futureValue
        assert(ex.isInstanceOf[IllegalStateException])
        assert(ex.getMessage.contains("closed"))
    }

    // ----- helpers -----

    private def decodeOutbound(messages: Seq[ByteString]): Seq[LocalTxMonitorMessage] =
        messages.map { msg =>
            val arr = msg.bytes
            given OriginalCborByteArray = OriginalCborByteArray(arr)
            io.bullet.borer.Cbor.decode(arr).to[LocalTxMonitorMessage].value
        }
}
