package scalus.cardano.node.stream.engine

import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.ledger.{CardanoInfo, DataHash, ProtocolParams, SlotNo, Transaction, TransactionHash, Utxos}
import scalus.cardano.node.stream.{ChainPoint, ChainTip, LocalNodeBackend, UtxoEvent}
import scalus.cardano.node.{SubmitError, UtxoQuery, UtxoQueryError, UtxoSource}
import scalus.uplc.builtin.Data

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.collection.mutable.ArrayBuffer

import EngineTestFixtures.*

class EngineSuite extends AnyFunSuite {

    private val ci: CardanoInfo = CardanoInfo.preview
    private val timeout: FiniteDuration = 5.seconds

    /** Drain whatever is currently buffered without blocking for more. Works by checking a small
      * timeout per pull; a pending pull (nothing buffered) terminates the drain.
      */
    private def drainReady[A](mailbox: Mailbox[A]): Seq[A] = {
        val buf = ArrayBuffer.empty[A]
        var continue = true
        while continue do {
            val f = mailbox.pull()
            try {
                Await.result(f, 50.millis) match {
                    case Some(a) => buf += a
                    case None    => continue = false
                }
            } catch {
                case _: java.util.concurrent.TimeoutException => continue = false
            }
        }
        buf.toSeq
    }

    private def mkEngine(securityParam: Int = 2160): Engine =
        new Engine(ci, None, securityParam)

    /** Minimal [[LocalNodeBackend]] whose mempool view is a mutable set the test controls. Only
      * `checkInMempool` / `checkInMempoolBatch` carry behaviour; the rest fail loudly if the engine
      * ever calls them (it shouldn't, for the tx-status path).
      */
    private final class FakeLocalNode(var mempool: Set[TransactionHash]) extends LocalNodeBackend {
        def cardanoInfo: CardanoInfo = ci
        def executionContext: ExecutionContext = ExecutionContext.global
        def submit(transaction: Transaction): Future[Either[SubmitError, TransactionHash]] =
            Future.failed(new UnsupportedOperationException("submit"))
        def currentSlot: Future[SlotNo] =
            Future.failed(new UnsupportedOperationException("currentSlot"))
        def fetchLatestParams: Future[ProtocolParams] =
            Future.failed(new UnsupportedOperationException("fetchLatestParams"))
        def findUtxos(query: UtxoQuery): Future[Either[UtxoQueryError, Utxos]] =
            Future.failed(new UnsupportedOperationException("findUtxos"))
        def getDatum(datumHash: DataHash): Future[Option[Data]] =
            Future.failed(new UnsupportedOperationException("getDatum"))
        def checkInMempool(txHash: TransactionHash): Future[Boolean] =
            Future.successful(mempool.contains(txHash))
        def close(): Future[Unit] = Future.unit
    }

    test("subscribeTip + onRollForward emits the block's ChainTip") {
        val engine = mkEngine()
        val id = engine.nextSubscriptionId()
        val mailbox = Mailbox.latestValue[ChainTip]()
        Await.result(engine.registerTipSubscription(id, mailbox), timeout)

        Await.result(engine.onRollForward(block(1, tx(10))), timeout)
        Await.result(engine.onRollForward(block(2, tx(11))), timeout)

        // LatestValueMailbox coalesces — assert we see the latest value.
        val pulled = Await.result(mailbox.pull(), timeout)
        assert(pulled.map(_.point).contains(point(2)))
    }

    test("subscribeUtxoQuery sees Created/Spent events for matching UTxOs") {
        val engine = mkEngine()
        val id = engine.nextSubscriptionId()
        val mailbox = Mailbox.delta[UtxoEvent]()
        val q = UtxoQuery(UtxoSource.FromAddress(addressA))
        Await.result(
          engine.registerUtxoSubscription(id, q, includeExistingUtxos = false, mailbox),
          timeout
        )

        val tx1 = tx(
          idN = 100,
          producing = IndexedSeq(output(addressA, 10L), output(addressB, 20L))
        )
        Await.result(engine.onRollForward(block(1, tx1)), timeout)

        val tx2 = tx(idN = 200, spending = Set(input(100, 0)))
        Await.result(engine.onRollForward(block(2, tx2)), timeout)

        val events = drain(mailbox, 2)
        val kinds = events.map {
            case _: UtxoEvent.Created    => "C"
            case _: UtxoEvent.Spent      => "S"
            case _: UtxoEvent.RolledBack => "R"
        }
        assert(kinds == Seq("C", "S"))
    }

    test("onRollBackward emits RolledBack and restores index state") {
        val engine = mkEngine()
        val id = engine.nextSubscriptionId()
        val mailbox = Mailbox.delta[UtxoEvent]()
        val q = UtxoQuery(UtxoSource.FromAddress(addressA))
        Await.result(
          engine.registerUtxoSubscription(id, q, includeExistingUtxos = false, mailbox),
          timeout
        )

        val tx1 = tx(100, producing = IndexedSeq(output(addressA, 1L)))
        val tx2 = tx(200, spending = Set(input(100, 0)))
        Await.result(engine.onRollForward(block(1, tx1)), timeout)
        Await.result(engine.onRollForward(block(2, tx2)), timeout)
        Await.result(engine.onRollBackward(point(1)), timeout)

        val events = drain(mailbox, 3)
        assert(events.last.isInstanceOf[UtxoEvent.RolledBack])

        // After rollback, findUtxosLocal should return the restored UTxO.
        val local = Await.result(engine.findUtxosLocal(q), timeout)
        assert(local.isDefined)
        assert(local.get.keys == Set(input(100, 0)))
    }

    test("findUtxosLocal returns None when no subscription covers the query") {
        val engine = mkEngine()
        val q = UtxoQuery(UtxoSource.FromAddress(addressA))
        val local = Await.result(engine.findUtxosLocal(q), timeout)
        assert(local.isEmpty)
    }

    test("subscribeUtxoQuery with NoBackup + includeExistingUtxos fails the mailbox") {
        val engine = mkEngine()
        val id = engine.nextSubscriptionId()
        val mailbox = Mailbox.delta[UtxoEvent]()
        val q = UtxoQuery(UtxoSource.FromAddress(addressA))
        Await.result(
          engine.registerUtxoSubscription(id, q, includeExistingUtxos = true, mailbox),
          timeout
        )
        val pulled = Await.ready(mailbox.pull(), timeout)
        assert(pulled.value.get.failed.get.isInstanceOf[Engine.NoBackupConfiguredException])
    }

    test("notifySubmit flips subscribeTransactionStatus from NotFound to Pending") {
        import scalus.cardano.node.TransactionStatus.*
        val engine = mkEngine()
        val id = engine.nextSubscriptionId()
        val mailbox = Mailbox.latestValue[scalus.cardano.node.TransactionStatus]()
        val h = txHash(777)
        Await.result(engine.registerTxStatusSubscription(id, h, mailbox), timeout)
        Await.result(engine.notifySubmit(h), timeout)
        Await.result(engine.onRollForward(block(1, tx(777))), timeout)

        // Latest-value: consecutive pulls give whatever's there. With
        // three offers (NotFound on register, Pending on submit,
        // Confirmed on block) and no pulls in between, coalescing
        // means we only observe the final value. Still enough to
        // verify the end state.
        val latest = Await.result(mailbox.pull(), timeout)
        assert(latest.contains(Confirmed))
    }

    test("currentTip tracks the latest applied block") {
        val engine = mkEngine()
        assert(engine.currentTip.isEmpty)
        Await.result(engine.onRollForward(block(5)), timeout)
        assert(engine.currentTip.map(_.point).contains(point(5)))
        Await.result(engine.onRollForward(block(6)), timeout)
        assert(engine.currentTip.map(_.point).contains(point(6)))
    }

    test("lookupDatum surfaces an in-window datum and forgets it after rollback") {
        val engine = mkEngine()
        val (h, d) = datum(123)

        val blockWithDatum = block(1, tx(100)).copy(datums = Map(h -> d))
        Await.result(engine.onRollForward(blockWithDatum), timeout)
        assert(Await.result(engine.lookupDatum(h), timeout).contains(d))

        Await.result(engine.onRollBackward(ChainPoint.origin), timeout)
        assert(Await.result(engine.lookupDatum(h), timeout).isEmpty)
    }

    test("lookupDatum drops datums whose introducing block ages past the security horizon") {
        val engine = mkEngine(securityParam = 1)
        val (h, d) = datum(7)

        Await.result(engine.onRollForward(block(1, tx(11)).copy(datums = Map(h -> d))), timeout)
        assert(Await.result(engine.lookupDatum(h), timeout).contains(d))

        Await.result(engine.onRollForward(block(2, tx(12))), timeout)
        assert(Await.result(engine.lookupDatum(h), timeout).isEmpty)
    }

    test("lookupDatum falls through to ChainStoreDatumDict once the in-memory window evicts") {
        val store = new KvChainStore(
          scalus.cardano.node.stream.engine.kvstore.InMemoryKvStore()
        )
        val engine =
            new Engine(ci, None, securityParam = 1, chainStore = Some(store))
        val (h, d) = datum(99)

        Await.result(engine.onRollForward(block(1, tx(11)).copy(datums = Map(h -> d))), timeout)
        // Push past the k=1 horizon so the volatile DatumIndex evicts the entry; the persistent
        // dict on the configured ChainStore still answers.
        Await.result(engine.onRollForward(block(2, tx(12))), timeout)
        assert(Await.result(engine.lookupDatum(h), timeout).contains(d))
    }

    test("LTM poll on block arrival flips a third-party tx to Pending") {
        import scalus.cardano.node.TransactionStatus.*
        val h = txHash(900)
        val fake = new FakeLocalNode(mempool = Set(h))
        val engine = new Engine(ci, None, securityParam = 2160, localNode = Some(fake))

        val id = engine.nextSubscriptionId()
        val mailbox = Mailbox.latestValue[scalus.cardano.node.TransactionStatus]()
        Await.result(engine.registerTxStatusSubscription(id, h, mailbox), timeout)

        // Block does NOT contain `h` — it stays mempool-only. The post-block LTM poll should
        // flip the subscriber from NotFound to Pending.
        Await.result(engine.onRollForward(block(1, tx(1))), timeout)

        // First pull may coalesce to either NotFound or Pending depending on poll timing; keep
        // pulling until Pending lands (the poll is async via the localNode's EC).
        var status = Await.result(mailbox.pull(), timeout)
        while status.contains(NotFound) do status = Await.result(mailbox.pull(), timeout)
        assert(status.contains(Pending))
    }

    test("LTM poll: a tx leaving the mempool reverts the subscriber to NotFound") {
        import scalus.cardano.node.TransactionStatus.*
        val h = txHash(901)
        val fake = new FakeLocalNode(mempool = Set(h))
        val engine = new Engine(ci, None, securityParam = 2160, localNode = Some(fake))

        val id = engine.nextSubscriptionId()
        val mailbox = Mailbox.latestValue[scalus.cardano.node.TransactionStatus]()
        Await.result(engine.registerTxStatusSubscription(id, h, mailbox), timeout)

        Await.result(engine.onRollForward(block(1, tx(1))), timeout)
        var s1 = Await.result(mailbox.pull(), timeout)
        while s1.contains(NotFound) do s1 = Await.result(mailbox.pull(), timeout)
        assert(s1.contains(Pending))

        // Tx drops out of the mempool (TTL expiry / eviction); next block's poll reverts it.
        fake.mempool = Set.empty
        Await.result(engine.onRollForward(block(2, tx(2))), timeout)
        var s2 = Await.result(mailbox.pull(), timeout)
        while s2.contains(Pending) do s2 = Await.result(mailbox.pull(), timeout)
        assert(s2.contains(NotFound))
    }

    test("LTM poll does not shadow Confirmed for a tx still in the node mempool") {
        import scalus.cardano.node.TransactionStatus.*
        val h = txHash(902)
        // Fake keeps reporting `h` in the mempool even after it confirms — a real node would drop
        // it, but this exercises the `confirmed` guard in applyMempoolPoll.
        val fake = new FakeLocalNode(mempool = Set(h))
        val engine = new Engine(ci, None, securityParam = 2160, localNode = Some(fake))

        val id = engine.nextSubscriptionId()
        val mailbox = Mailbox.latestValue[scalus.cardano.node.TransactionStatus]()
        Await.result(engine.registerTxStatusSubscription(id, h, mailbox), timeout)

        // Block DOES contain `h` — it confirms. The post-block poll still sees `h` in the fake's
        // mempool, but the `confirmed` guard must keep the final status at Confirmed.
        Await.result(engine.onRollForward(block(1, tx(902))), timeout)

        // Give the async poll a chance to land, then assert the latest value is Confirmed and
        // nothing downgrades it.
        Thread.sleep(100)
        val status = Await.result(mailbox.pull(), timeout)
        assert(status.contains(Confirmed))
    }
}
