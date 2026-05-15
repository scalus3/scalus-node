package scalus.cardano.network.it

import cats.effect.IO
import cats.effect.std.Dispatcher
import cats.effect.unsafe.implicits.global
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import scalus.cardano.ledger.CardanoInfo
import scalus.cardano.network.NetworkMagic
import scalus.cardano.node.stream.fs2.Fs2BlockchainStreamProvider
import scalus.cardano.node.stream.{BackupSource, ChainSyncSource, StorageProfile, StreamProviderConfig}
import scalus.testing.yaci.YaciDevKit

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

/** End-to-end smoke IT for the M8 TxSubmission2 wiring. Spins up an N2N connection through the
  * full provider stack — which now also spawns a [[TxSubmission2Driver]] on the
  * `MiniProtocolId.TxSubmission` channel — and asserts chain-sync continues to advance against
  * the peer for several seconds.
  *
  * What this proves: the producer side of TxSubmission2 (our `MsgInit` and any responses to peer
  * requests) is at least *protocol-conforming* — a malformed message on that channel would cause
  * the peer to tear down the entire `NodeToNodeConnection` at the multiplexer level, and chain
  * sync would die with it. Tips continuing to arrive over a multi-second window is therefore
  * strong (if indirect) evidence the wire is right.
  *
  * What this does NOT cover: an end-to-end "submit a real transaction → observe it in a block"
  * test. That needs a Conway-era transaction builder (or a yaci helper) — out of scope for M8
  * Phase 3 and tracked as a follow-up.
  *
  * Runs under `sbt scalusCardanoNetworkIt/test`. Requires Docker.
  */
class YaciN2nSubmitSuite
    extends AnyFunSuite
    with YaciDevKit
    with ScalaFutures
    with Eventually {

    implicit override val patienceConfig: PatienceConfig =
        PatienceConfig(timeout = Span(120, Seconds), interval = Span(500, Millis))

    test("TxSubmission2 driver runs alongside chain-sync without tripping the peer") {
        val host = container.getHost
        val port = container.getCardanoNodePort

        given ExecutionContext = ExecutionContext.global

        Dispatcher.parallel[IO].use { d =>
            given Dispatcher[IO] = d

            val config = StreamProviderConfig(
              appId = "scalus.it.yaci-n2n-submit",
              cardanoInfo = CardanoInfo.preview,
              chainSync = ChainSyncSource.N2N(host, port, NetworkMagic.YaciDevnet.value),
              backup = BackupSource.NoBackup,
              storage = StorageProfile.Light(
                scalus.cardano.node.stream.engine.persistence.EnginePersistenceStore.noop
              )
            )

            // Run for a multi-second window so any TxSubmission2 wire violation that the peer
            // disconnects on would have stopped tip delivery by the time we assert.
            val tipsObserved = new AtomicLong(0L)

            for {
                provider <- Fs2BlockchainStreamProvider.create(config)
                outcome <- provider
                    .subscribeTip()
                    .evalMap(_ => IO { tipsObserved.incrementAndGet(); () })
                    .interruptAfter(10.seconds)
                    .compile
                    .drain
                    .attempt
                _ <- provider.close()
            } yield {
                outcome match {
                    case Left(t) =>
                        fail(
                          s"chain-sync stream failed while TxSubmission2 was active: " +
                              s"${t.getClass.getSimpleName}: ${t.getMessage} " +
                              s"(tipsObserved=${tipsObserved.get})",
                          t
                        )
                    case Right(_) => ()
                }
                assert(
                  tipsObserved.get >= 3L,
                  s"expected ≥ 3 tips in a 10s window with TxSubmission2 active; got " +
                      s"${tipsObserved.get} — peer may have torn down the connection over a " +
                      s"TxSubmission2 wire violation"
                )
            }
        }.unsafeRunSync()
    }
}
