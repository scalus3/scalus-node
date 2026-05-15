package scalus.examples.streaming.fs2batcher

import cats.effect.IO
import cats.effect.std.Dispatcher
import cats.effect.unsafe.IORuntime
import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.ledger.{CardanoInfo, DataHash, ProtocolParams, SlotNo, Transaction, TransactionHash, Utxos}
import scalus.uplc.builtin.Data
import scalus.cardano.node.{BlockchainProvider, NetworkSubmitError, SubmitError, TransactionStatus, UtxoQuery, UtxoQueryError, UtxoSource}
import scalus.cardano.node.stream.{N2nTxSubmissionBackend, StartFrom, SubscriptionOptions, SyntheticEventSource, UtxoEvent, UtxoEventQuery}
import scalus.cardano.node.stream.engine.{Engine, EngineTestFixtures}
import scalus.cardano.node.stream.fs2.{Fs2BlockchainStreamProvider, Fs2ScalusAsyncStream}

import Fs2ScalusAsyncStream.IOStream
import EngineTestFixtures.*

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*

/** Verifies that snapshot-method fallbacks route to [[BackupSource.Custom]] when the engine can't
  * answer locally, and that [[BackupSource.NoBackup]] yields typed errors.
  */
class BackupDelegationSuite extends AnyFunSuite {

    private given IORuntime = IORuntime.global
    private val ci: CardanoInfo = CardanoInfo.preview
    private val awaitTimeout: FiniteDuration = 5.seconds

    /** Records every call and returns pre-programmed responses. */
    private final class StubBackup(
        val findUtxosResult: Either[UtxoQueryError, Utxos],
        val submitResult: Either[SubmitError, TransactionHash],
        val checkTxResult: TransactionStatus = TransactionStatus.NotFound,
        val getDatumResult: Option[Data] = None
    )(using val ec: ExecutionContext)
        extends BlockchainProvider {

        var submittedTxs: List[Transaction] = Nil
        var findUtxosQueries: List[UtxoQuery] = Nil
        var checkTxCalls: List[TransactionHash] = Nil
        var getDatumCalls: List[DataHash] = Nil

        override def executionContext: ExecutionContext = ec
        override def cardanoInfo: CardanoInfo = ci
        override def currentSlot: Future[SlotNo] = Future.successful(100L)
        override def fetchLatestParams: Future[ProtocolParams] =
            Future.successful(ci.protocolParams)
        override def findUtxos(query: UtxoQuery): Future[Either[UtxoQueryError, Utxos]] = {
            findUtxosQueries = query :: findUtxosQueries
            Future.successful(findUtxosResult)
        }
        override def checkTransaction(
            txHash: TransactionHash
        ): Future[TransactionStatus] = {
            checkTxCalls = txHash :: checkTxCalls
            Future.successful(checkTxResult)
        }
        override def submit(
            transaction: Transaction
        ): Future[Either[SubmitError, TransactionHash]] = {
            submittedTxs = transaction :: submittedTxs
            Future.successful(submitResult)
        }
        override def getDatum(datumHash: DataHash): Future[Option[Data]] = {
            getDatumCalls = datumHash :: getDatumCalls
            Future.successful(getDatumResult)
        }
    }

    private def withProvider[T](backup: Option[BlockchainProvider])(
        f: (
            Fs2BlockchainStreamProvider & SyntheticEventSource[IO, IOStream],
            ExecutionContext
        ) => IO[T]
    ): T =
        Dispatcher
            .parallel[IO]
            .use { d =>
                given Dispatcher[IO] = d
                given ExecutionContext = ExecutionContext.global
                val provider = new Fs2BlockchainStreamProvider(
                  new Engine(ci, backup, Engine.DefaultSecurityParam)
                ) with SyntheticEventSource[IO, IOStream]
                f(provider, summon[ExecutionContext])
            }
            .unsafeRunSync()

    test("findUtxos falls through to backup when no subscription covers the query") {
        val stubUtxos = Map(input(1, 0) -> output(addressA, 42L))
        given ExecutionContext = ExecutionContext.global
        val backup = new StubBackup(
          findUtxosResult = Right(stubUtxos),
          submitResult = Left(NetworkSubmitError.ConnectionError("unused"))
        )
        val result = withProvider(Some(backup)) { (provider, _) =>
            provider.findUtxos(UtxoQuery(UtxoSource.FromAddress(addressA)))
        }
        assert(result == Right(stubUtxos))
        assert(backup.findUtxosQueries.size == 1)
    }

    test("submit delegates to backup and records hash in pendingOwnSubmissions") {
        given ExecutionContext = ExecutionContext.global
        val hash = txHash(500)
        val backup = new StubBackup(
          findUtxosResult = Left(UtxoQueryError.NotFound(UtxoSource.FromAddress(addressA))),
          submitResult = Right(hash)
        )
        val output = withProvider(Some(backup)) { (provider, _) =>
            val dummyTx = Transaction.empty
            for {
                _ <- provider.submit(dummyTx)
                status <- provider.checkTransaction(hash)
            } yield status
        }
        assert(output == TransactionStatus.Pending)
        assert(backup.submittedTxs.size == 1)
    }

    test("NoBackup: findUtxos returns typed NotFound") {
        val result = withProvider(None) { (provider, _) =>
            provider.findUtxos(UtxoQuery(UtxoSource.FromAddress(addressA)))
        }
        result match {
            case Left(UtxoQueryError.NotFound(UtxoSource.FromAddress(addr))) =>
                assert(addr == addressA)
            case other => fail(s"expected NotFound(FromAddress), got $other")
        }
    }

    test("submit falls through to N2nTxSubmissionBackend when no Blockfrost or LocalNode") {
        given ExecutionContext = ExecutionContext.global
        val hash = txHash(700)
        val submittedTxs = scala.collection.mutable.ArrayBuffer.empty[Transaction]
        val n2nStub: N2nTxSubmissionBackend = new N2nTxSubmissionBackend {
            override def submit(
                tx: Transaction
            ): Future[Either[SubmitError, TransactionHash]] = {
                submittedTxs += tx
                Future.successful(Right(hash))
            }
            override def close(): Future[Unit] = Future.unit
        }
        val output: Either[SubmitError, TransactionHash] = Dispatcher
            .parallel[IO]
            .use { d =>
                given Dispatcher[IO] = d
                given ExecutionContext = ExecutionContext.global
                val provider = new Fs2BlockchainStreamProvider(
                  new Engine(
                    ci,
                    None,
                    Engine.DefaultSecurityParam,
                    txSubmission = Some(n2nStub)
                  )
                ) with SyntheticEventSource[IO, IOStream]
                provider.submit(Transaction.empty)
            }
            .unsafeRunSync()
        assert(output == Right(hash))
        assert(submittedTxs.size == 1)
    }

    test("NoBackup: submit returns a typed ConnectionError") {
        val result = withProvider(None) { (provider, _) =>
            provider.submit(Transaction.empty)
        }
        result match {
            case Left(NetworkSubmitError.ConnectionError(msg, _)) =>
                assert(msg.contains("no backup"))
            case other => fail(s"expected ConnectionError, got $other")
        }
    }

    test("getDatum returns the in-engine datum without consulting the backup") {
        given ExecutionContext = ExecutionContext.global
        val backup = new StubBackup(
          findUtxosResult = Left(UtxoQueryError.NotFound(UtxoSource.FromAddress(addressA))),
          submitResult = Left(NetworkSubmitError.ConnectionError("unused"))
        )
        val datum: Data = Data.I(BigInt(99))
        val datumHashV =
            DataHash.fromByteString(scalus.uplc.builtin.Data.dataHash(datum))
        val blockWithDatum =
            block(1, tx(10)).copy(datums = Map(datumHashV -> datum))
        val result = withProvider(Some(backup)) { (provider, _) =>
            IO.fromFuture(IO(provider.engine.onRollForward(blockWithDatum))) >>
                provider.getDatum(datumHashV)
        }
        assert(result.contains(datum))
        assert(backup.getDatumCalls.isEmpty)
    }

    test("getDatum falls through to backup on a miss in the engine") {
        given ExecutionContext = ExecutionContext.global
        val datum: Data = Data.I(BigInt(7))
        val datumHashV =
            DataHash.fromByteString(scalus.uplc.builtin.Data.dataHash(datum))
        val backup = new StubBackup(
          findUtxosResult = Left(UtxoQueryError.NotFound(UtxoSource.FromAddress(addressA))),
          submitResult = Left(NetworkSubmitError.ConnectionError("unused")),
          getDatumResult = Some(datum)
        )
        val result = withProvider(Some(backup)) { (provider, _) =>
            provider.getDatum(datumHashV)
        }
        assert(result.contains(datum))
        assert(backup.getDatumCalls == List(datumHashV))
    }

    test("getDatum without a backup returns None on a miss") {
        val datum: Data = Data.I(BigInt(11))
        val datumHashV =
            DataHash.fromByteString(scalus.uplc.builtin.Data.dataHash(datum))
        val result = withProvider(None) { (provider, _) =>
            provider.getDatum(datumHashV)
        }
        assert(result.isEmpty)
    }

    test("subscribeUtxoQuery with includeExistingUtxos=true seeds from backup") {
        given ExecutionContext = ExecutionContext.global
        val seedUtxos = Map(input(9, 0) -> output(addressA, 1000L))
        val backup = new StubBackup(
          findUtxosResult = Right(seedUtxos),
          submitResult = Left(NetworkSubmitError.ConnectionError("unused"))
        )
        val collected = withProvider(Some(backup)) { (provider, _) =>
            val q = UtxoEventQuery(UtxoQuery(UtxoSource.FromAddress(addressA)))
            val opts = SubscriptionOptions(
              startFrom = StartFrom.Tip,
              includeExistingUtxos = true
            )
            val stream = provider.subscribeUtxoQuery(q, opts)
            stream.take(1).compile.toList
        }
        assert(collected.size == 1)
        collected.head match {
            case UtxoEvent.Created(utxo, _, _) =>
                assert(utxo.input == input(9, 0))
            case other => fail(s"expected Created seed event, got $other")
        }
    }

    test("backupDiagnostics surfaces a snapshot when the backup implements the trait") {
        given ExecutionContext = ExecutionContext.global
        val opaqueHash = txHash(7)
        val diagBackup = new BlockchainProvider with scalus.cardano.node.stream.BackupDiagnostics {
            override def executionContext: ExecutionContext = summon[ExecutionContext]
            override def cardanoInfo: CardanoInfo = ci
            override def currentSlot: Future[SlotNo] = Future.successful(0L)
            override def fetchLatestParams: Future[ProtocolParams] =
                Future.successful(ci.protocolParams)
            override def findUtxos(query: UtxoQuery): Future[Either[UtxoQueryError, Utxos]] =
                Future.successful(Left(UtxoQueryError.NotFound(UtxoSource.FromAddress(addressA))))
            override def checkTransaction(txHash: TransactionHash): Future[TransactionStatus] =
                Future.successful(TransactionStatus.NotFound)
            override def submit(
                transaction: Transaction
            ): Future[Either[SubmitError, TransactionHash]] =
                Future.successful(Left(NetworkSubmitError.ConnectionError("unused")))
            override def getDatum(datumHash: DataHash): Future[Option[Data]] =
                Future.successful(None)
            def diagnostics: scalus.cardano.node.stream.BackupDiagnosticsSnapshot =
                scalus.cardano.node.stream.BackupDiagnosticsSnapshot(
                  connectedSinceMillis = 1700_000_000_000L,
                  lastSubmittedHash = Some(opaqueHash),
                  submitCount = 3L,
                  rejectCount = 1L
                )
        }
        val snap = withProvider(Some(diagBackup)) { (provider, _) =>
            IO.pure(provider.backupDiagnostics)
        }
        assert(snap.isDefined)
        assert(snap.get.connectedSinceMillis == 1700_000_000_000L)
        assert(snap.get.lastSubmittedHash.contains(opaqueHash))
        assert(snap.get.submitCount == 3L)
        assert(snap.get.rejectCount == 1L)
    }

    test("backupDiagnostics returns None when the configured backup omits the trait") {
        given ExecutionContext = ExecutionContext.global
        val plainBackup = new StubBackup(
          findUtxosResult = Left(UtxoQueryError.NotFound(UtxoSource.FromAddress(addressA))),
          submitResult = Left(NetworkSubmitError.ConnectionError("unused"))
        )
        val snap = withProvider(Some(plainBackup)) { (provider, _) =>
            IO.pure(provider.backupDiagnostics)
        }
        assert(snap.isEmpty)
    }

    test("backupDiagnostics returns None when no backup is configured") {
        val snap = withProvider(None) { (provider, _) =>
            IO.pure(provider.backupDiagnostics)
        }
        assert(snap.isEmpty)
    }
}
