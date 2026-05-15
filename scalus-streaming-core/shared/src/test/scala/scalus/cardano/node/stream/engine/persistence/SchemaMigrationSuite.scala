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

    test("decodeMigrating folds a multi-step synthetic chain to the current version") {
        // Two synthetic steps 0 → 1 → 2 — each only bumps the version field. Exercises the
        // chain-fold logic with more than one step (a single-step chain would not).
        val v0Snap = snapshotAt(0).copy(appId = "test.app.migrated", networkMagic = 99L)
        val v0Bytes = PersistenceCodecs.encodeSnapshot(v0Snap)

        def bump(toVersion: Int): SchemaMigration.Migration = { bs =>
            val decoded = PersistenceCodecs.decodeSnapshot(bs)
            PersistenceCodecs.encodeSnapshot(decoded.copy(schemaVersion = toVersion))
        }
        val migrations: Map[Int, SchemaMigration.Migration] =
            Map(0 -> bump(1), 1 -> bump(EngineSnapshotFile.CurrentSchemaVersion))

        val result = SchemaMigration.decodeMigrating(v0Bytes, migrations)
        assert(result.schemaVersion == EngineSnapshotFile.CurrentSchemaVersion)
        assert(result.appId == "test.app.migrated")
        assert(result.networkMagic == 99L)
    }

    test("decodeMigrating upgrades a real v1 snapshot to the current version (v2)") {
        // End-to-end test of the production v1 → v2 migration via the package-private encoder
        // on the frozen v1 type. Result should be a v2 EngineSnapshotFile with generation = 0.
        import EngineSnapshotFileV1.given
        import io.bullet.borer.Cbor
        val v1 = EngineSnapshotFileV1(
          schemaVersion = 1,
          appId = "test.app.v1",
          networkMagic = 7L,
          tip = None,
          ownSubmissions = Set.empty,
          volatileTail = Seq.empty,
          buckets = Map.empty
        )
        val v1Bytes = Cbor.encode(v1).toByteArray

        val result = SchemaMigration.decodeMigrating(v1Bytes)
        assert(result.schemaVersion == EngineSnapshotFile.CurrentSchemaVersion)
        assert(result.appId == "test.app.v1")
        assert(result.networkMagic == 7L)
        assert(result.generation == 0L)
    }
}
