package scalus.cardano.network.it

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import scalus.cardano.ledger.TransactionHash
import scalus.cardano.network.n2c.LocalNodeAccess
import scalus.uplc.builtin.ByteString

import scala.concurrent.ExecutionContext.Implicits.global

/** End-to-end LocalTxMonitor IT against a live cardano-node, reached through yaci-devkit's socat
  * n2c bridge ([[YaciN2cContainer]]).
  *
  * Asserts the mempool-snapshot path for hashes the node has never seen — a submit→observe test
  * would need a funded wallet + tx builder and is left for a follow-up. Runs under
  * `sbt scalusCardanoNetworkIt/test`. Requires Docker.
  */
class YaciLocalTxMonitorSuite extends AnyFunSuite with YaciN2cAccess with ScalaFutures {

    implicit override val patienceConfig: PatienceConfig =
        PatienceConfig(timeout = Span(60, Seconds), interval = Span(200, Millis))

    private def withProvider(test: LocalNodeAccess => Unit): Unit = {
        val provider = connectLocalNode()
        try test(provider)
        finally provider.close().futureValue
    }

    private def hashOf(byte: Int): TransactionHash =
        TransactionHash.fromByteString(ByteString.fromArray(Array.fill[Byte](32)(byte.toByte)))

    test("checkInMempool returns false for a hash the node has never seen") {
        withProvider { p =>
            assert(!p.checkInMempool(hashOf(0x55)).futureValue)
        }
    }

    test("checkInMempoolBatch returns an empty set for all-unknown hashes") {
        withProvider { p =>
            val unknowns = Set(hashOf(0x11), hashOf(0x22), hashOf(0x33))
            assert(p.checkInMempoolBatch(unknowns).futureValue.isEmpty)
        }
    }

    test("checkInMempoolBatch on an empty input set short-circuits to empty") {
        withProvider { p =>
            assert(p.checkInMempoolBatch(Set.empty).futureValue.isEmpty)
        }
    }
}
