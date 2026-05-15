package scalus.cardano.node.stream.engine.persistence

import io.bullet.borer.{Cbor, Dom}

/** Schema-migration mechanism for [[EngineSnapshotFile]] — lets a library upgrade read a snapshot
  * written by an older format version instead of forcing a wipe-and-cold-start.
  *
  * The loader peeks the on-disk `schemaVersion` (CBOR array field 0) without committing to a
  * decoder, then:
  *   - `== CurrentSchemaVersion` — decode directly.
  *   - `>  CurrentSchemaVersion` — [[EnginePersistenceError.SchemaMismatch]]: a file written by a
  *     newer library cannot be migrated down, the future format is unknown.
  *   - `<  CurrentSchemaVersion` — fold the migration chain `vN → vN+1 → … → current`. Each step is
  *     a pure `Array[Byte] => Array[Byte]` re-encode, implemented over frozen per-version codecs so
  *     it never depends on the current `EngineSnapshotFile` shape.
  *
  * [[migrations]] is empty today — the M6 format is the only version. The first real entry lands
  * with M14.C, which bumps the snapshot to v2. Until then a `< current` version is unreachable in
  * practice and an unknown one fails `SchemaMismatch` like any missing step.
  */
object SchemaMigration {

    /** One `vN → vN+1` step. */
    type Migration = Array[Byte] => Array[Byte]

    /** Migration steps keyed by *source* version. */
    private val migrations: Map[Int, Migration] = Map(
      // v1 (M6) → v2 (M14.C, adds `generation`). Decode with the frozen v1 codec, supply
      // `generation = 0`, re-encode with the current (v2) codec.
      1 -> { bytes =>
          import EngineSnapshotFileV1.given
          val v1 = Cbor.decode(bytes).to[EngineSnapshotFileV1].value
          PersistenceCodecs.encodeSnapshot(v1.toV2)
      }
    )

    /** Read the on-disk `schemaVersion` without committing to the rest of the decode. Returns the
      * first int of the snapshot's outer CBOR array.
      *
      * Implemented over [[Dom.Element]] — the codebase's proven partial-read primitive (see
      * `LocalStateQueryMessage.captureRawCbor`). Decoding the snapshot file to `Dom` once at
      * startup is acceptable: the file is read once per process start, and the typical snapshot is
      * engine-local state (tip + buckets + tail), not chain history.
      */
    def peekVersion(bytes: Array[Byte]): Int = {
        val root = Cbor.decode(bytes).to[Dom.Element].value
        val first = root match {
            case Dom.ArrayElem.Sized(elems) if elems.nonEmpty   => elems.head
            case Dom.ArrayElem.Unsized(elems) if elems.nonEmpty => elems.head
            case other =>
                throw new IllegalArgumentException(
                  s"snapshot is not a non-empty CBOR array: ${other.getClass.getSimpleName}"
                )
        }
        first match {
            case Dom.IntElem(v)  => v
            case Dom.LongElem(v) => v.toInt
            case other =>
                throw new IllegalArgumentException(
                  s"snapshot schemaVersion field is not an int: ${other.getClass.getSimpleName}"
                )
        }
    }

    /** Decode `bytes`, migrating from its on-disk version up to
      * [[EngineSnapshotFile.CurrentSchemaVersion]].
      */
    def decodeMigrating(bytes: Array[Byte]): EngineSnapshotFile =
        decodeMigrating(bytes, migrations)

    /** Test seam — same as [[decodeMigrating(bytes:Array[Byte])*]] but accepts an explicit
      * registry, so unit tests can exercise the chain-fold logic with synthetic versions before any
      * real migration step lands.
      */
    private[persistence] def decodeMigrating(
        bytes: Array[Byte],
        migrations: Map[Int, Migration]
    ): EngineSnapshotFile = {
        val version = peekVersion(bytes)
        val current = EngineSnapshotFile.CurrentSchemaVersion
        if version == current then PersistenceCodecs.decodeSnapshot(bytes)
        else if version > current then throw EnginePersistenceError.SchemaMismatch(version, current)
        else applyChain(version, current, bytes, migrations)
    }

    private def applyChain(
        from: Int,
        to: Int,
        bytes: Array[Byte],
        migrations: Map[Int, Migration]
    ): EngineSnapshotFile = {
        var v = from
        var b = bytes
        while v < to do
            migrations.get(v) match {
                case Some(step) =>
                    b = step(b)
                    v += 1
                case None =>
                    throw EnginePersistenceError.SchemaMismatch(from, to)
            }
        PersistenceCodecs.decodeSnapshot(b)
    }
}
