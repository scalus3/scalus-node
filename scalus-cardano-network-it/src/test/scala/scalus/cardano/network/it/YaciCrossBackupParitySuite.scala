package scalus.cardano.network.it

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import scalus.cardano.address.Address
import scalus.cardano.ledger.Utxos
import scalus.cardano.network.n2c.LocalNodeAccess
import scalus.cardano.node.{BlockfrostProvider, UtxoQuery, UtxoSource}

import scala.concurrent.ExecutionContext.Implicits.global

/** Tier-5 cross-backup parity IT: the same read queries against `BackupSource.LocalNode` (the N2C
  * socat bridge) and `BackupSource.Blockfrost` (yaci-store's HTTP API), both pointed at the *same*
  * yaci-devkit node. Catches silent divergence between the two backends — e.g. a decoder that
  * agrees with hand-rolled golden bytes but not with what the node actually serves.
  *
  * Runs under `sbt scalusCardanoNetworkIt/test`. Requires Docker.
  */
class YaciCrossBackupParitySuite extends AnyFunSuite with YaciN2cAccess with ScalaFutures {

    implicit override val patienceConfig: PatienceConfig =
        PatienceConfig(timeout = Span(60, Seconds), interval = Span(200, Millis))

    private lazy val fundedAddress: Address =
        Address.fromBech32(YaciN2cContainer.FundedAddress)

    /** A valid testnet address yaci never funds (one of yaci's own commented-out example
      * addresses) — both backends should resolve it to an empty UTxO set.
      */
    private lazy val unusedAddress: Address = Address.fromBech32(
      "addr_test1qqwpl7h3g84mhr36wpetk904p7fchx2vst0z696lxk8ujsjyruqwmlsm344gfux3nsj6njyzj3ppvrqtt36cp9xyydzqzumz82"
    )

    private def connectBlockfrost(): BlockfrostProvider =
        BlockfrostProvider
            .localYaci(
              // `localYaci` concatenates `baseUrl + "/epochs/..."` — the container's URLs come
              // with a trailing slash, so strip it to avoid a `//` in the path (→ 404).
              YaciN2cContainer.container.getYaciStoreApiUrl.stripSuffix("/"),
              YaciN2cContainer.container.getLocalClusterApiUrl.stripSuffix("/")
            )
            .futureValue

    private def withProviders(test: (LocalNodeAccess, BlockfrostProvider) => Unit): Unit = {
        val localNode = connectLocalNode()
        val blockfrost = connectBlockfrost()
        try test(localNode, blockfrost)
        finally localNode.close().futureValue
    }

    private def totalLovelace(utxos: Utxos): BigInt =
        utxos.values.map(o => BigInt(o.value.coin.value)).sum

    test("currentSlot agrees across backends (within a small tolerance)") {
        withProviders { (localNode, blockfrost) =>
            val lnSlot = localNode.currentSlot.futureValue
            val bfSlot = blockfrost.currentSlot.futureValue
            assert(lnSlot > 0L, s"localNode slot=$lnSlot")
            assert(bfSlot > 0L, s"blockfrost slot=$bfSlot")
            // LocalNode reads the LSQ chain tip; Blockfrost derives the slot from wall-clock +
            // the devnet slot config. With 1s slots they track closely — allow generous slack
            // for indexer lag / scheduling jitter.
            assert(
              math.abs(lnSlot - bfSlot) <= 30L,
              s"slot divergence too large: localNode=$lnSlot blockfrost=$bfSlot"
            )
        }
    }

    test("fetchLatestParams agrees across backends on the stable fields") {
        withProviders { (localNode, blockfrost) =>
            val ln = localNode.fetchLatestParams.futureValue
            val bf = blockfrost.fetchLatestParams.futureValue
            assert(ln.txFeePerByte == bf.txFeePerByte, s"txFeePerByte ${ln.txFeePerByte}/${bf.txFeePerByte}")
            assert(ln.txFeeFixed == bf.txFeeFixed, s"txFeeFixed ${ln.txFeeFixed}/${bf.txFeeFixed}")
            assert(ln.maxTxSize == bf.maxTxSize, s"maxTxSize ${ln.maxTxSize}/${bf.maxTxSize}")
            assert(
              ln.maxBlockBodySize == bf.maxBlockBodySize,
              s"maxBlockBodySize ${ln.maxBlockBodySize}/${bf.maxBlockBodySize}"
            )
            assert(
              ln.stakeAddressDeposit == bf.stakeAddressDeposit,
              s"stakeAddressDeposit ${ln.stakeAddressDeposit}/${bf.stakeAddressDeposit}"
            )
            assert(
              ln.stakePoolDeposit == bf.stakePoolDeposit,
              s"stakePoolDeposit ${ln.stakePoolDeposit}/${bf.stakePoolDeposit}"
            )
            assert(
              ln.utxoCostPerByte == bf.utxoCostPerByte,
              s"utxoCostPerByte ${ln.utxoCostPerByte}/${bf.utxoCostPerByte}"
            )
            assert(
              ln.protocolVersion == bf.protocolVersion,
              s"protocolVersion ${ln.protocolVersion}/${bf.protocolVersion}"
            )
        }
    }

    test("findUtxos by-address agrees on the funded fixture address") {
        withProviders { (localNode, blockfrost) =>
            val q = UtxoQuery(UtxoSource.FromAddress(fundedAddress))
            val ln = localNode.findUtxos(q).futureValue
            val bf = blockfrost.findUtxos(q).futureValue
            assert(ln.isRight, s"localNode findUtxos failed: $ln")
            assert(bf.isRight, s"blockfrost findUtxos failed: $bf")
            val lnUtxos = ln.toOption.get
            val bfUtxos = bf.toOption.get
            assert(lnUtxos.nonEmpty, "expected the funded address to have UTxOs")
            // The funding tx lands once and is never spent, so both backends must agree on the
            // exact set of TxIns and the total lovelace held.
            assert(
              lnUtxos.keySet == bfUtxos.keySet,
              s"TxIn keyset divergence:\n  localNode=${lnUtxos.keySet}\n  blockfrost=${bfUtxos.keySet}"
            )
            assert(
              totalLovelace(lnUtxos) == totalLovelace(bfUtxos),
              s"total lovelace divergence: localNode=${totalLovelace(lnUtxos)} " +
                  s"blockfrost=${totalLovelace(bfUtxos)}"
            )
        }
    }

    test("findUtxos by-address agrees that an unused address is empty") {
        withProviders { (localNode, blockfrost) =>
            val q = UtxoQuery(UtxoSource.FromAddress(unusedAddress))
            val ln = localNode.findUtxos(q).futureValue
            val bf = blockfrost.findUtxos(q).futureValue
            assert(ln.exists(_.isEmpty), s"localNode expected empty, got $ln")
            assert(bf.exists(_.isEmpty), s"blockfrost expected empty, got $bf")
        }
    }
}
