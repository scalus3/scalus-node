package scalus.cardano.node.stream.engine.persistence

import io.bullet.borer.{Decoder, Reader}
import scalus.cardano.ledger.TransactionHash
import scalus.cardano.node.stream.ChainTip
import scalus.cardano.node.stream.engine.UtxoKey

import PersistenceCodecs.given

/** Frozen v1 shape of [[EngineSnapshotFile]] — the M6 layout with no `generation` field. Used by
  * the [[SchemaMigration]] registry's `1 → 2` step. The encoder is intentionally absent (v1 is no
  * longer written by this library); only the decoder is needed to read legacy files.
  */
private[persistence] final case class EngineSnapshotFileV1(
    schemaVersion: Int,
    appId: String,
    networkMagic: Long,
    tip: Option[ChainTip],
    ownSubmissions: Set[TransactionHash],
    volatileTail: Seq[AppliedBlockSummary],
    buckets: Map[UtxoKey, BucketState]
) {
    def toV2: EngineSnapshotFile = EngineSnapshotFile(
      schemaVersion = EngineSnapshotFile.CurrentSchemaVersion,
      appId = appId,
      networkMagic = networkMagic,
      tip = tip,
      ownSubmissions = ownSubmissions,
      volatileTail = volatileTail,
      buckets = buckets,
      generation = 0L
    )
}

private[persistence] object EngineSnapshotFileV1 {

    given Decoder[EngineSnapshotFileV1] with
        def read(r: Reader): EngineSnapshotFileV1 = {
            val len = r.readArrayHeader().toInt
            if len != 7 then r.validationFailure(s"EngineSnapshotFileV1 arrLen=$len (expected 7)")
            val schemaVersion = r.readInt()
            val appId = r.readString()
            val networkMagic = r.readLong()
            val tipLen = r.readArrayHeader().toInt
            val tip = tipLen match {
                case 0 => None
                case 1 => Some(r.read[ChainTip]())
                case n => r.validationFailure(s"v1 tip option arrLen=$n (expected 0 or 1)")
            }
            val subsN = r.readArrayHeader().toInt
            val subsB = Set.newBuilder[TransactionHash]
            var i = 0
            while i < subsN do { subsB += r.read[TransactionHash](); i += 1 }
            val tailN = r.readArrayHeader().toInt
            val tailB = Seq.newBuilder[AppliedBlockSummary]
            i = 0
            while i < tailN do { tailB += r.read[AppliedBlockSummary](); i += 1 }
            val bucketsN = r.readArrayHeader().toInt
            val bucketsB = Map.newBuilder[UtxoKey, BucketState]
            i = 0
            while i < bucketsN do {
                val pairLen = r.readArrayHeader().toInt
                if pairLen != 2 then r.validationFailure(s"v1 buckets pair arrLen=$pairLen")
                bucketsB += (r.read[UtxoKey]() -> r.read[BucketState]())
                i += 1
            }
            EngineSnapshotFileV1(
              schemaVersion,
              appId,
              networkMagic,
              tip,
              subsB.result(),
              tailB.result(),
              bucketsB.result()
            )
        }
}
