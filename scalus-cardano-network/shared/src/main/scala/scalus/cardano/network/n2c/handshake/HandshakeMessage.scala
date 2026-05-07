package scalus.cardano.network.n2c.handshake

import io.bullet.borer.{Decoder, Encoder, Reader, Writer}
import scalus.cardano.network.NetworkMagic

import scala.collection.immutable.SortedMap

/** Node-to-Client version data per the ouroboros-network CDDL
  * (`node-to-client-version-data-v16+.cddl`).
  *
  * For v16 and newer the shape is a 2-element array `[networkMagic, query]`. The optional field is
  * `query :: Bool` — `true` enables LocalStateQuery semantics on the connection. M11 always
  * proposes `query = false`; M12 raises it.
  *
  * Older N2C version data shapes (v9..v15) are intentionally not modelled here — M11 negotiates v16
  * minimum, and a peer that only supports older versions will be refused via
  * [[RefuseReason.VersionMismatch]].
  */
final case class NodeToClientVersionData(
    networkMagic: NetworkMagic,
    query: Boolean
)

/** Refuse reason for the N2C handshake — same wire shape as N2N (CDDL is identical) but kept
  * separate from the N2N type so the handshake error model isn't cross-coupled to the transport the
  * caller actually opened.
  */
sealed trait RefuseReason

object RefuseReason {
    final case class VersionMismatch(peerSupported: List[Int]) extends RefuseReason
    final case class HandshakeDecodeError(version: Int, message: String) extends RefuseReason
    final case class Refused(version: Int, message: String) extends RefuseReason
}

/** CBOR map from version number to per-version data. Same wire layout as the N2N table — keys are
  * `u32`, values are per-version arrays — but the value shape depends on the *transport*, not the
  * version, so we keep N2N and N2C tables as sibling types.
  */
type VersionTable = SortedMap[Int, NodeToClientVersionData]

object VersionTable {
    def apply(entries: (Int, NodeToClientVersionData)*): VersionTable = SortedMap.from(entries)
}

/** Top-level N2C handshake message ADT. Shape is identical to the N2N handshake (CDDL diffs are
  * only in the per-version data inside the table) but the message type is kept separate so an
  * UnexpectedMessage error from one transport can't accidentally surface a value from the other.
  *
  * {{{
  * handshakeMessage   = msgProposeVersions / msgAcceptVersion / msgRefuse / msgQueryReply
  * msgProposeVersions = [0, versionTable]
  * msgAcceptVersion   = [1, versionNumber, versionData]
  * msgRefuse          = [2, refuseReason]
  * msgQueryReply      = [3, versionTable]
  * }}}
  */
sealed trait HandshakeMessage

object HandshakeMessage {
    final case class MsgProposeVersions(table: VersionTable) extends HandshakeMessage
    final case class MsgAcceptVersion(version: Int, data: NodeToClientVersionData)
        extends HandshakeMessage
    final case class MsgRefuse(reason: RefuseReason) extends HandshakeMessage
    final case class MsgQueryReply(table: VersionTable) extends HandshakeMessage

    given Encoder[NodeToClientVersionData] with
        def write(w: Writer, d: NodeToClientVersionData): Writer =
            w.writeArrayHeader(2)
                .writeLong(d.networkMagic.value)
                .writeBoolean(d.query)

    private def readVersionData(r: Reader): NodeToClientVersionData = {
        val elems = r.readArrayHeader().toInt
        if elems != 2 then
            r.validationFailure(s"unexpected N2C versionData shape: elems=$elems (expected 2)")
        NodeToClientVersionData(NetworkMagic(r.readLong()), r.readBoolean())
    }

    private def writeVersionTable(w: Writer, table: VersionTable): Writer = {
        w.writeMapHeader(table.size)
        table.foreach { case (version, data) =>
            w.writeLong(version.toLong)
            w.write(data)
        }
        w
    }

    private def readVersionTable(r: Reader): VersionTable = {
        val entries = r.readMapHeader().toInt
        val builder = SortedMap.newBuilder[Int, NodeToClientVersionData]
        var i = 0
        while i < entries do {
            val version = r.readInt()
            val data = readVersionData(r)
            builder += (version -> data)
            i += 1
        }
        builder.result()
    }

    given Encoder[RefuseReason] with
        def write(w: Writer, reason: RefuseReason): Writer = reason match {
            case RefuseReason.VersionMismatch(versions) =>
                w.writeArrayHeader(2).writeInt(0).writeArrayHeader(versions.size)
                versions.foreach(v => w.writeInt(v))
                w
            case RefuseReason.HandshakeDecodeError(version, message) =>
                w.writeArrayHeader(3).writeInt(1).writeInt(version).writeString(message)
            case RefuseReason.Refused(version, message) =>
                w.writeArrayHeader(3).writeInt(2).writeInt(version).writeString(message)
        }

    given Decoder[RefuseReason] with
        def read(r: Reader): RefuseReason = {
            val arrLen = r.readArrayHeader().toInt
            r.readInt() match {
                case 0 if arrLen == 2 =>
                    val n = r.readArrayHeader().toInt
                    val builder = List.newBuilder[Int]
                    var i = 0
                    while i < n do {
                        builder += r.readInt()
                        i += 1
                    }
                    RefuseReason.VersionMismatch(builder.result())
                case 1 if arrLen == 3 =>
                    RefuseReason.HandshakeDecodeError(r.readInt(), r.readString())
                case 2 if arrLen == 3 =>
                    RefuseReason.Refused(r.readInt(), r.readString())
                case other =>
                    r.validationFailure(s"unexpected refuseReason tag=$other arrLen=$arrLen")
            }
        }

    given Encoder[HandshakeMessage] with
        def write(w: Writer, m: HandshakeMessage): Writer = m match {
            case MsgProposeVersions(table) =>
                w.writeArrayHeader(2).writeInt(0)
                writeVersionTable(w, table)
            case MsgAcceptVersion(version, data) =>
                w.writeArrayHeader(3).writeInt(1).writeLong(version.toLong).write(data)
            case MsgRefuse(reason) =>
                w.writeArrayHeader(2).writeInt(2).write(reason)
            case MsgQueryReply(table) =>
                w.writeArrayHeader(2).writeInt(3)
                writeVersionTable(w, table)
        }

    given Decoder[HandshakeMessage] with
        def read(r: Reader): HandshakeMessage = {
            val arrLen = r.readArrayHeader().toInt
            r.readInt() match {
                case 0 if arrLen == 2 => MsgProposeVersions(readVersionTable(r))
                case 1 if arrLen == 3 =>
                    val version = r.readInt()
                    MsgAcceptVersion(version, readVersionData(r))
                case 2 if arrLen == 2 => MsgRefuse(summon[Decoder[RefuseReason]].read(r))
                case 3 if arrLen == 2 => MsgQueryReply(readVersionTable(r))
                case other =>
                    r.validationFailure(
                      s"unexpected n2c handshakeMessage tag=$other arrLen=$arrLen"
                    )
            }
        }
}
