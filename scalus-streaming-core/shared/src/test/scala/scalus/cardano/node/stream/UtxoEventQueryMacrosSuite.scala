package scalus.cardano.node.stream

import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.address.Address
import scalus.cardano.ledger.Coin
import scalus.cardano.node.{UtxoFilter, UtxoQuery, UtxoSource}

import UtxoEventQueryMacros.buildEventQuery

/** Demo suite for the streaming-side HOAS macro [[UtxoEventQueryMacros.buildEventQuery]]. Each test
  * pairs a lambda call with the structured `UtxoEventQuery` it must lower to.
  */
class UtxoEventQueryMacrosSuite extends AnyFunSuite {

    private val addrA: Address = Address.fromBech32(
      "addr1qx2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3n0d3vllmyqwsx5wktcd8cc3sq835lu7drv2xwl2wywfgse35a3x"
    )

    test("address + Created kind → query with Set(Created)") {
        val q = buildEventQuery { (u, kind) =>
            u.output.address == addrA && kind == UtxoEventType.Created
        }
        assert(
          q == UtxoEventQuery(
            UtxoQuery.Simple(UtxoSource.FromAddress(addrA)),
            Set(UtxoEventType.Created)
          )
        )
    }

    test("address + Spent kind → query with Set(Spent)") {
        val q = buildEventQuery { (u, kind) =>
            u.output.address == addrA && kind == UtxoEventType.Spent
        }
        assert(
          q == UtxoEventQuery(
            UtxoQuery.Simple(UtxoSource.FromAddress(addrA)),
            Set(UtxoEventType.Spent)
          )
        )
    }

    test("kind side absent → all event types") {
        val q = buildEventQuery { (u, _) => u.output.address == addrA }
        assert(
          q == UtxoEventQuery(
            UtxoQuery.Simple(UtxoSource.FromAddress(addrA)),
            UtxoEventType.all
          )
        )
    }

    test("kind != X negates to the complement set") {
        val q = buildEventQuery { (u, kind) =>
            u.output.address == addrA && kind != UtxoEventType.Spent
        }
        assert(
          q == UtxoEventQuery(
            UtxoQuery.Simple(UtxoSource.FromAddress(addrA)),
            UtxoEventType.all - UtxoEventType.Spent
          )
        )
    }

    test("OR-chain on kind unions the resulting type sets") {
        val q = buildEventQuery { (u, kind) =>
            u.output.address == addrA &&
            (kind == UtxoEventType.Created || kind == UtxoEventType.Spent)
        }
        assert(
          q == UtxoEventQuery(
            UtxoQuery.Simple(UtxoSource.FromAddress(addrA)),
            Set(UtxoEventType.Created, UtxoEventType.Spent)
          )
        )
    }

    test("utxo-side AND-chain delegates filters through the upstream macro") {
        val q = buildEventQuery { (u, kind) =>
            u.output.address == addrA &&
            u.output.value.coin >= Coin(1_000_000) &&
            kind == UtxoEventType.Created
        }
        // The upstream macro folds the address source and the coin floor into a Simple+filter
        // query; we just check that the kind set + the source survived the round-trip.
        q match
            case UtxoEventQuery(
                  UtxoQuery.Simple(UtxoSource.FromAddress(a), filter, _, _, _),
                  kinds
                ) =>
                assert(a == addrA)
                assert(kinds == Set(UtxoEventType.Created))
                assert(filter.contains(UtxoFilter.MinLovelace(Coin(1_000_000))))
            case other => fail(s"expected Simple(FromAddress, MinLovelace), got $other")
    }
}
