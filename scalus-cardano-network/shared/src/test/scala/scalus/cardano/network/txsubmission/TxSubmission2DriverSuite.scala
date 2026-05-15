package scalus.cardano.network.txsubmission

import io.bullet.borer.Cbor
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import scalus.cardano.infra.CancelSource
import scalus.cardano.ledger.TransactionHash
import scalus.cardano.network.infra.ScriptedMiniProtocolBytes
import scalus.cardano.network.txsubmission.TxSubmission2Message.*
import scalus.uplc.builtin.ByteString

import scala.concurrent.{ExecutionContext, Future}

class TxSubmission2DriverSuite extends AnyFunSuite with ScalaFutures {

    implicit override val patienceConfig: PatienceConfig =
        PatienceConfig(timeout = Span(2, Seconds), interval = Span(5, Millis))

    private given ExecutionContext = ExecutionContext.global

    private def hashOf(byte: Int): TransactionHash =
        TransactionHash.fromArray(Array.fill[Byte](32)(byte.toByte))

    private def bodyOf(byte: Int, len: Int = 8): ByteString =
        ByteString.fromArray(Array.fill[Byte](len)(byte.toByte))

    /** Decode every message the driver has emitted so far. Each `sentOutbound` entry is a
      * standalone CBOR-encoded `TxSubmission2Message` (the test double's `send` just records the
      * full bytes that would have been framed onto the wire).
      */
    private def decodeAllSent(
        handle: ScriptedMiniProtocolBytes[TxSubmission2Message]
    ): Seq[TxSubmission2Message] =
        handle.sentOutbound.toSeq.map(bs => Cbor.decode(bs.bytes).to[TxSubmission2Message].value)

    /** Spin until `cond` becomes true (driver writes are async — `sentOutbound` updates after the
      * `await(stream.send(...))` resolves). Bounded by the patience timeout.
      */
    private def awaitCond(cond: => Boolean, hint: String): Unit = {
        val deadline = System.currentTimeMillis() + 2000L
        while !cond && System.currentTimeMillis() < deadline do {
            Thread.sleep(5)
        }
        assert(cond, hint)
    }

    private def newDriver(
        handle: ScriptedMiniProtocolBytes[TxSubmission2Message]
    ): (TxSubmission2Driver, Future[Unit], CancelSource) = {
        val root = CancelSource()
        val driver = new TxSubmission2Driver(handle, root.token)
        val done = driver.run()
        (driver, done, root)
    }

    /** Production-equivalent teardown — fire the connection root. The driver's `receive` aborts
      * with `CancelledException`, the async loop unwinds, the `transformWith` recovers it to `()`,
      * and the best-effort `MsgDone` is sent on the way out.
      */
    private def tearDown(root: CancelSource, done: Future[Unit]): Unit = {
        root.cancel(new RuntimeException("test teardown"))
        done.recover { case _ => () }.futureValue
    }

    test("driver sends MsgInit on start") {
        val handle = new ScriptedMiniProtocolBytes[TxSubmission2Message]
        val (_, done, root) = newDriver(handle)
        try {
            awaitCond(handle.sentOutbound.nonEmpty, "MsgInit should have been sent")
            assert(decodeAllSent(handle).head == MsgInit)
        } finally tearDown(root, done)
    }

    test("submitted ids surface in MsgReplyTxIds when peer asks (non-blocking)") {
        val handle = new ScriptedMiniProtocolBytes[TxSubmission2Message]
        val (driver, done, root) = newDriver(handle)
        try {
            driver.submitWire(hashOf(0x11), bodyOf(0xaa)).futureValue
            driver.submitWire(hashOf(0x22), bodyOf(0xbb)).futureValue

            handle.stage(MsgRequestTxIds(blocking = false, numAck = 0, numReq = 10))

            awaitCond(
              decodeAllSent(handle).exists(_.isInstanceOf[MsgReplyTxIds]),
              "MsgReplyTxIds should have been sent"
            )
            val reply = decodeAllSent(handle).collectFirst { case r: MsgReplyTxIds => r }.get
            assert(
              reply.txIds.map(_._1.hashBytes) == Seq(
                ByteString.fromArray(hashOf(0x11).bytes),
                ByteString.fromArray(hashOf(0x22).bytes)
              )
            )
            assert(reply.txIds.forall(_._1.era == TxSubmission2Driver.ConwayEra))
        } finally tearDown(root, done)
    }

    test("MsgRequestTxs returns the bodies for the requested ids in order") {
        val handle = new ScriptedMiniProtocolBytes[TxSubmission2Message]
        val (driver, done, root) = newDriver(handle)
        try {
            driver.submitWire(hashOf(0x11), bodyOf(0xaa)).futureValue
            driver.submitWire(hashOf(0x22), bodyOf(0xbb)).futureValue

            handle.stage(
              MsgRequestTxs(
                Seq(
                  TxId(TxSubmission2Driver.ConwayEra, ByteString.fromArray(hashOf(0x22).bytes)),
                  TxId(TxSubmission2Driver.ConwayEra, ByteString.fromArray(hashOf(0x11).bytes))
                )
              )
            )

            awaitCond(
              decodeAllSent(handle).exists(_.isInstanceOf[MsgReplyTxs]),
              "MsgReplyTxs should have been sent"
            )
            val reply = decodeAllSent(handle).collectFirst { case r: MsgReplyTxs => r }.get
            assert(reply.txs.map(_.txBytes) == Seq(bodyOf(0xbb), bodyOf(0xaa)))
        } finally tearDown(root, done)
    }

    test("blocking MsgRequestTxIds waits until a submit lands") {
        val handle = new ScriptedMiniProtocolBytes[TxSubmission2Message]
        val (driver, done, root) = newDriver(handle)
        try {
            awaitCond(handle.sentOutbound.nonEmpty, "MsgInit ready")
            val beforeReply = decodeAllSent(handle).count(_.isInstanceOf[MsgReplyTxIds])
            handle.stage(MsgRequestTxIds(blocking = true, numAck = 0, numReq = 3))

            // Driver should NOT reply yet — queue is empty.
            Thread.sleep(60)
            assert(
              decodeAllSent(handle).count(_.isInstanceOf[MsgReplyTxIds]) == beforeReply,
              "no MsgReplyTxIds expected while queue is empty"
            )

            // Now submit; the driver should wake and reply.
            driver.submitWire(hashOf(0x33), bodyOf(0xcc)).futureValue
            awaitCond(
              decodeAllSent(handle).count(_.isInstanceOf[MsgReplyTxIds]) == beforeReply + 1,
              "MsgReplyTxIds expected after submit unblocks the blocking request"
            )
            val reply = decodeAllSent(handle).collect { case r: MsgReplyTxIds => r }.last
            assert(reply.txIds.size == 1)
            assert(reply.txIds.head._1.hashBytes == ByteString.fromArray(hashOf(0x33).bytes))
        } finally tearDown(root, done)
    }

    test("numAck on a subsequent MsgRequestTxIds drops entries from the front") {
        val handle = new ScriptedMiniProtocolBytes[TxSubmission2Message]
        val (driver, done, root) = newDriver(handle)
        try {
            driver.submitWire(hashOf(0x11), bodyOf(0xaa)).futureValue
            driver.submitWire(hashOf(0x22), bodyOf(0xbb)).futureValue
            driver.submitWire(hashOf(0x33), bodyOf(0xcc)).futureValue

            // Drain everything once so the peer "sees" the offers, then ack two of them.
            handle.stage(MsgRequestTxIds(blocking = false, numAck = 0, numReq = 10))
            awaitCond(
              decodeAllSent(handle).count(_.isInstanceOf[MsgReplyTxIds]) == 1,
              "first reply"
            )

            handle.stage(MsgRequestTxIds(blocking = false, numAck = 2, numReq = 10))
            awaitCond(
              decodeAllSent(handle).count(_.isInstanceOf[MsgReplyTxIds]) == 2,
              "second reply"
            )

            // Only hashOf(0x33) should remain.
            assert(driver.pendingSubmissions == Seq(hashOf(0x33)))
            val secondReply = decodeAllSent(handle).collect { case r: MsgReplyTxIds => r }.last
            assert(secondReply.txIds.size == 1)
            assert(
              secondReply.txIds.head._1.hashBytes ==
                  ByteString.fromArray(hashOf(0x33).bytes)
            )
        } finally tearDown(root, done)
    }

    test("cancelling the connection root exits the loop cleanly (production teardown)") {
        // The same path the provider's `close()` will exercise: the connection root cancels, the
        // parked `receive` aborts with CancelledException, the async loop unwinds, the
        // transformWith recovers to (), and the best-effort MsgDone is sent on the way out.
        val handle = new ScriptedMiniProtocolBytes[TxSubmission2Message]
        val (_, done, root) = newDriver(handle)
        awaitCond(handle.sentOutbound.nonEmpty, "MsgInit ready")
        root.cancel(new RuntimeException("test cancel"))
        done.futureValue
        val sent = decodeAllSent(handle)
        assert(sent.contains(MsgInit))
        assert(sent.contains(MsgDone), "best-effort MsgDone should be emitted on cancel")
    }

    test("peer-sent MsgDone causes the loop to exit cleanly") {
        val handle = new ScriptedMiniProtocolBytes[TxSubmission2Message]
        val (_, done, root) = newDriver(handle)
        handle.stage(MsgDone)
        // No exception; future completes successfully.
        done.futureValue
        // And the driver sent MsgInit + a best-effort MsgDone on exit.
        val sent = decodeAllSent(handle)
        assert(sent.contains(MsgInit))
        assert(sent.contains(MsgDone))
    }

    test("submit after close fails synchronously") {
        val handle = new ScriptedMiniProtocolBytes[TxSubmission2Message]
        val (driver, done, root) = newDriver(handle)
        try {
            driver.close()
            val attempt = driver.submitWire(hashOf(0x77), bodyOf(0xdd))
            assert(attempt.failed.futureValue.isInstanceOf[IllegalStateException])
        } finally tearDown(root, done)
    }
}
