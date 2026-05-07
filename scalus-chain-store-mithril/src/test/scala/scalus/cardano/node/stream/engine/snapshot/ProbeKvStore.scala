package scalus.cardano.node.stream.engine.snapshot

import scalus.cardano.node.stream.engine.kvstore.{InMemoryKvStore, KvStore}
import scalus.cardano.node.stream.engine.kvstore.rocksdb.RocksDbKvStore

import java.nio.file.{Files, Path}

/** Backend chooser for `[manual]` restore probes.
  *
  * Default: a heap-resident [[InMemoryKvStore]]. Holds the full restored block history and UTxO set
  * in the JVM heap — fine for tiny smoke runs, but ~15+ GB RSS at preview scale and out of reach at
  * mainnet scale (the UTxO set alone won't fit in heap).
  *
  * Set `SCALUS_RESTORE_ROCKSDB_DIR=<path>` to back the same `KvChainStore` with a freshly allocated
  * [[RocksDbKvStore]] at `<path>` instead. The directory must be absent or empty:
  * `ImmutableDbRestorer` is documented as restoring into an empty store, and silently mixing into a
  * populated RocksDB would produce a corrupt state. We refuse rather than wipe — the caller decides
  * whether the existing data is safe to remove.
  */
object ProbeKvStore {

    val RocksDbDirEnv = "SCALUS_RESTORE_ROCKSDB_DIR"

    /** Result of [[open]] — pairs the underlying [[KvStore]] with a human-readable label so probes
      * can log which backend they ended up on.
      */
    final case class Opened(store: KvStore, label: String)

    def open(): Opened =
        sys.env.get(RocksDbDirEnv).map(Path.of(_)) match {
            case Some(dir) => Opened(openRocksDb(dir), s"RocksDB at $dir")
            case None      => Opened(InMemoryKvStore(), "InMemoryKvStore (heap)")
        }

    private def openRocksDb(dir: Path): KvStore = {
        if Files.exists(dir) then {
            require(Files.isDirectory(dir), s"$RocksDbDirEnv=$dir exists but is not a directory")
            val nonEmpty = scala.util.Using.resource(Files.list(dir))(_.findAny().isPresent)
            if nonEmpty then
                throw new IllegalStateException(
                  s"$RocksDbDirEnv=$dir is not empty; refusing to restore on top of an " +
                      "existing RocksDB. Wipe the directory or pick another path."
                )
        }
        RocksDbKvStore.open(dir)
    }
}
