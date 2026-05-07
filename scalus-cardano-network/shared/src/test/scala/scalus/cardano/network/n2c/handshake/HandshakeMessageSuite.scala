package scalus.cardano.network.n2c.handshake

import io.bullet.borer.Cbor
import org.scalatest.funsuite.AnyFunSuite
import scalus.cardano.network.NetworkMagic
import scalus.cardano.network.n2c.handshake.HandshakeMessage.*
import scalus.utils.Hex.*

/** CBOR round-trip + golden-vector tests for the N2C handshake message codec. Goldens were produced
  * by hand from the ouroboros-network N2C CDDL.
  */
class HandshakeMessageSuite extends AnyFunSuite {

    private def roundTrip(m: HandshakeMessage): Unit = {
        val bytes = Cbor.encode(m).toByteArray
        val decoded = Cbor.decode(bytes).to[HandshakeMessage].value
        assert(decoded == m, s"round-trip mismatch for $m\nbytes=${bytes.toHex}")
    }

    private def data(magic: Long = NetworkMagic.Mainnet.value, query: Boolean = false) =
        NodeToClientVersionData(NetworkMagic(magic), query = query)

    test("round-trip MsgProposeVersions with v16 only") {
        roundTrip(MsgProposeVersions(VersionTable(NodeToClientVersion.V16 -> data())))
    }

    test("round-trip MsgProposeVersions with v16..v20") {
        roundTrip(
          MsgProposeVersions(
            VersionTable(
              NodeToClientVersion.V16 -> data(),
              NodeToClientVersion.V17 -> data(),
              NodeToClientVersion.V18 -> data(),
              NodeToClientVersion.V19 -> data(),
              NodeToClientVersion.V20 -> data()
            )
          )
        )
    }

    test("round-trip MsgAcceptVersion at v16 with query=true") {
        roundTrip(MsgAcceptVersion(NodeToClientVersion.V16, data(query = true)))
    }

    test("round-trip MsgRefuse/VersionMismatch") {
        roundTrip(MsgRefuse(RefuseReason.VersionMismatch(List(14, 15, 16))))
    }

    test("round-trip MsgRefuse/HandshakeDecodeError") {
        roundTrip(MsgRefuse(RefuseReason.HandshakeDecodeError(16, "bad cbor")))
    }

    test("round-trip MsgRefuse/Refused") {
        roundTrip(MsgRefuse(RefuseReason.Refused(16, "magic mismatch")))
    }

    test("round-trip MsgQueryReply") {
        roundTrip(MsgQueryReply(VersionTable(NodeToClientVersion.V16 -> data(query = true))))
    }

    // ----------------------------------------------------------------------------------------
    // Golden vectors — hand-crafted from the N2C CDDL.
    // ----------------------------------------------------------------------------------------

    test("golden: MsgProposeVersions(v16 only, Mainnet magic, query=false)") {
        // [0, {16: [764824073, false]}]
        //
        // 82                array(2)
        //   00              uint 0                    ; tag for MsgProposeVersions
        //   A1              map(1)
        //     10            uint 16                   ; key: version 16
        //     82            array(2)
        //       1A 2D964A09 uint 764824073            ; Mainnet magic
        //       F4          false                     ; query
        val expected = "82 00 a1 10 82 1a 2d 96 4a 09 f4".filter(_ != ' ')
        val m: HandshakeMessage =
            MsgProposeVersions(VersionTable(NodeToClientVersion.V16 -> data()))
        val bytes = Cbor.encode[HandshakeMessage](m).toByteArray
        assert(
          bytes.toHex == expected,
          s"expected=$expected\n     got=${bytes.toHex}"
        )
        val decoded = Cbor.decode(expected.hexToBytes).to[HandshakeMessage].value
        assert(decoded == m)
    }

    test("golden: MsgAcceptVersion at v16 (Mainnet, query=true)") {
        // [1, 16, [764824073, true]]
        //
        // 83                array(3)
        //   01              uint 1                    ; tag for MsgAcceptVersion
        //   10              uint 16                   ; versionNumber
        //   82              array(2)
        //     1A 2D964A09   uint 764824073
        //     F5            true
        val expected = "83 01 10 82 1a 2d 96 4a 09 f5".filter(_ != ' ')
        val m: HandshakeMessage =
            MsgAcceptVersion(NodeToClientVersion.V16, data(query = true))
        val bytes = Cbor.encode[HandshakeMessage](m).toByteArray
        assert(
          bytes.toHex == expected,
          s"expected=$expected\n     got=${bytes.toHex}"
        )
    }

    test("golden: MsgRefuse/VersionMismatch([14, 15, 16])") {
        // [2, [0, [14, 15, 16]]]
        val expected = "82 02 82 00 83 0e 0f 10".filter(_ != ' ')
        val m: HandshakeMessage = MsgRefuse(RefuseReason.VersionMismatch(List(14, 15, 16)))
        val bytes = Cbor.encode[HandshakeMessage](m).toByteArray
        assert(bytes.toHex == expected)
    }

    // ----------------------------------------------------------------------------------------
    // Error paths
    // ----------------------------------------------------------------------------------------

    test("decode rejects unknown tag") {
        val bytes = Array[Byte](0x82.toByte, 0x07, 0x00)
        val ex = intercept[Exception] { Cbor.decode(bytes).to[HandshakeMessage].value }
        assert(ex.getMessage.contains("tag=7") || ex.getMessage.toLowerCase.contains("handshake"))
    }

    test("decode rejects 5-element versionData (N2N shape) at v16") {
        // MsgAcceptVersion with version=16 but 5-element data (N2N v16 shape).
        // 83 01 10 85 02 F5 00 F4 F4
        val bytes = Array[Byte](
          0x83.toByte,
          0x01,
          0x10,
          0x85.toByte,
          0x02,
          0xf5.toByte,
          0x00,
          0xf4.toByte,
          0xf4.toByte
        )
        val ex = intercept[Exception] { Cbor.decode(bytes).to[HandshakeMessage].value }
        assert(ex.getMessage.toLowerCase.contains("versiondata"))
    }
}
