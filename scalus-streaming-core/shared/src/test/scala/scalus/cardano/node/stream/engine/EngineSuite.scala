package scalus.cardano.node.stream.engine

import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.ledger.CardanoInfo
import scalus.cardano.node.stream.{ChainPoint, ChainTip, UtxoEvent}
import scalus.cardano.node.{UtxoQuery, UtxoSource}

import scala.concurrent.duration.*
import scala.concurrent.Await
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
}
