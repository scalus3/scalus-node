package scalus.cardano.node.stream.engine.persistence

import org.scalatest.funsuite.AnyFunSuite

class SchemaMigrationSuite extends AnyFunSuite {

    private def snapshotAt(version: Int): EngineSnapshotFile =
        EngineSnapshotFile(
          schemaVersion = version,
          appId = "test.app",
          networkMagic = 42L,
          tip = None,
          ownSubmissions = Set.empty,
          volatileTail = Seq.empty,
          buckets = Map.empty
        )

    test("peekVersion reads the schemaVersion field without full decode") {
        val bytes = PersistenceCodecs.encodeSnapshot(snapshotAt(7))
        assert(SchemaMigration.peekVersion(bytes) == 7)
    }

    test("decodeMigrating passes through a current-version snapshot unchanged") {
        val snap = snapshotAt(EngineSnapshotFile.CurrentSchemaVersion)
        val bytes = PersistenceCodecs.encodeSnapshot(snap)
        assert(SchemaMigration.decodeMigrating(bytes) == snap)
    }

    test("decodeMigrating fails SchemaMismatch on a newer-than-current snapshot") {
        val bytes = PersistenceCodecs.encodeSnapshot(
          snapshotAt(EngineSnapshotFile.CurrentSchemaVersion + 1)
        )
        intercept[EnginePersistenceError.SchemaMismatch] {
            SchemaMigration.decodeMigrating(bytes)
        }
    }

    test("decodeMigrating fails SchemaMismatch when older and no migration is registered") {
        // version 0 is older than CurrentSchemaVersion = 1; the production registry is empty.
        val bytes = PersistenceCodecs.encodeSnapshot(snapshotAt(0))
        intercept[EnginePersistenceError.SchemaMismatch] {
            SchemaMigration.decodeMigrating(bytes)
        }
    }

    test("decodeMigrating folds a synthetic migration chain to the current version") {
        // Synthetic v0 → v1 step: re-encode at the current schemaVersion. Exercises the chain-fold
        // before any real migration ships — the production migrations map stays empty until M14.C.
        val v0Snap = snapshotAt(0).copy(appId = "test.app.migrated", networkMagic = 99L)
        val v0Bytes = PersistenceCodecs.encodeSnapshot(v0Snap)

        val migrations: Map[Int, SchemaMigration.Migration] = Map(
          0 -> { bs =>
              val decoded = PersistenceCodecs.decodeSnapshot(bs)
              PersistenceCodecs.encodeSnapshot(
                decoded.copy(schemaVersion = EngineSnapshotFile.CurrentSchemaVersion)
              )
          }
        )

        val result = SchemaMigration.decodeMigrating(v0Bytes, migrations)
        assert(result.schemaVersion == EngineSnapshotFile.CurrentSchemaVersion)
        assert(result.appId == "test.app.migrated")
        assert(result.networkMagic == 99L)
    }
}
