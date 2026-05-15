package scalus.cardano.node.stream.engine.persistence

import io.bullet.borer.Cbor

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.{FileChannel, FileLock, OverlappingFileLockException}
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

import PersistenceCodecs.given

/** File-backed [[EnginePersistenceStore]].
  *
  * Layout for `appId` = "com.foo.bar":
  *
  * {{{
  * <dataRoot>/scalus-stream/com.foo.bar/
  *   com.foo.bar.snapshot        ← last compacted state (absent on first run)
  *   com.foo.bar.log             ← append-only journal since the last compaction
  *   com.foo.bar.lock            ← process-exclusion `FileLock`
  * }}}
  *
  * The journal file carries a 16-byte fixed-binary header (MAGIC + schema version + generation)
  * stamped to match the snapshot's `generation`. Mismatched (or missing) header ⇒ the journal is
  * discarded on load with a warning — making the rename-then-truncate sequence in [[compact]]
  * crash-atomic without a lock-step protocol: whatever point a crash lands on, a mismatched journal
  * is exactly one whose records the snapshot already subsumes.
  *
  * Runtime compaction kicks in when the journal grows past
  * [[FileEnginePersistenceStore.DefaultCompactionThresholdBytes]] — surfaced via [[compactionDue]]
  * and triggered by the engine after each [[appendSync]]. M14.B / M14.C.
  *
  * See `docs/local/claude/indexer/engine-persistence-minimal.md` for the M6 durability contract and
  * `engine-persistence-advanced-m14.md` for M14.B + M14.C.
  */
final class FileEnginePersistenceStore private (
    private val dir: Path,
    private val baseName: String,
    private val fsyncInterval: FiniteDuration,
    private val bufferCapacityBytes: Int,
    private val compactionThresholdBytes: Long
)(using ec: ExecutionContext)
    extends EnginePersistenceStore {

    import FileEnginePersistenceStore.*

    private val snapshotPath: Path = dir.resolve(s"$baseName.snapshot")
    private val logPath: Path = dir.resolve(s"$baseName.log")
    private val lockPath: Path = dir.resolve(s"$baseName.lock")

    private val lockChannel: FileChannel =
        FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    private val fileLock: FileLock =
        try
            Option(lockChannel.tryLock()).getOrElse {
                lockChannel.close()
                throw EnginePersistenceError.Locked(lockPath.toString)
            }
        catch {
            case _: OverlappingFileLockException =>
                lockChannel.close()
                throw EnginePersistenceError.Locked(lockPath.toString)
            case NonFatal(t) =>
                lockChannel.close()
                throw EnginePersistenceError.Io(t)
        }

    private val logChannel: FileChannel = FileChannel.open(
      logPath,
      StandardOpenOption.CREATE,
      StandardOpenOption.READ,
      StandardOpenOption.WRITE
    )

    private val buffer: ByteBuffer = ByteBuffer.allocate(bufferCapacityBytes)
    private val closed: AtomicBoolean = new AtomicBoolean(false)

    /** Cached snapshot — read once at construction; reused by [[load]] so the file is only parsed
      * once per store lifetime. `None` ⇒ no snapshot file (first run).
      */
    private val cachedSnapshot: Option[EngineSnapshotFile] =
        if Files.exists(snapshotPath) then Some(readSnapshot())
        else None

    /** The generation the file store is currently writing under. Initialised from the cached
      * snapshot; bumped on every successful [[compact]].
      */
    private val currentGenerationRef: AtomicLong =
        new AtomicLong(cachedSnapshot.map(_.generation).getOrElse(0L))

    /** Byte counter for threshold-triggered compaction. Incremented after each successful
      * [[appendSync]] write; reset to 0 on [[compact]].
      */
    private val journalBytesSinceCompactionRef: AtomicLong = new AtomicLong(0L)

    // Validate the journal header (or write a fresh one) before any appends position to end.
    initializeJournalHeader(currentGenerationRef.get)
    logChannel.position(logChannel.size())

    // Scheduled fsync ticker. One thread per store — cheap because the store's lifetime matches
    // the engine's.
    private val scheduler: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(r => {
            val t = new Thread(r, s"scalus-stream-persistence-ticker-$baseName")
            t.setDaemon(true)
            t
        })
    private val tickerHandle: ScheduledFuture[?] = scheduler.scheduleAtFixedRate(
      () => { val _ = flushInternal() },
      fsyncInterval.toMillis,
      fsyncInterval.toMillis,
      TimeUnit.MILLISECONDS
    )

    // ------------------------------------------------------------------

    def load(): Future[Option[PersistedEngineState]] = Future {
        val records: Seq[JournalRecord] = loadJournal()
        if cachedSnapshot.isEmpty && records.isEmpty then None
        else Some(PersistedEngineState(cachedSnapshot, records))
    }

    def appendSync(record: JournalRecord): Unit = {
        if closed.get() then return
        val bytes = Cbor.encode(record).toByteArray
        val required = 4 + bytes.length
        try
            this.synchronized {
                if buffer.remaining() < required then writeBuffer()
                if required > buffer.capacity() then {
                    // Oversized record — flush any buffered data first, then write directly.
                    writeDirect(bytes)
                } else {
                    buffer.putInt(bytes.length)
                    buffer.put(bytes)
                }
            }
            val _ = journalBytesSinceCompactionRef.addAndGet(required.toLong)
        catch {
            case NonFatal(t) =>
                // Swallow — by contract, transient append errors are logged and surface via the
                // next flush/compact. Here we have no logger in this scope; a System.err line is
                // acceptable for M6 and keeps the "never silently eat exceptions" invariant.
                System.err.println(
                  s"scalus-stream: appendSync failed on $logPath — ${t.getClass.getSimpleName}: ${t.getMessage}"
                )
        }
    }

    def flush(): Future[Unit] = Future(flushInternal())

    override def compactionDue: Boolean =
        journalBytesSinceCompactionRef.get >= compactionThresholdBytes

    def compact(snap: EngineSnapshotFile): Future[Unit] = Future {
        this.synchronized {
            flushInternal()
            val nextGen = currentGenerationRef.get + 1L
            val stamped = snap.copy(generation = nextGen)
            val tmp = dir.resolve(s"$baseName.snapshot.tmp")
            val bytes = Cbor.encode(stamped).toByteArray
            Files.write(
              tmp,
              bytes,
              StandardOpenOption.CREATE,
              StandardOpenOption.TRUNCATE_EXISTING,
              StandardOpenOption.WRITE
            )
            // Atomic rename over the old snapshot. After this point a crash leaves the new snapshot
            // beside a stale (gen N) journal; on next start the generation check discards it,
            // correctly — because the snapshot already subsumes those records.
            Files.move(
              tmp,
              snapshotPath,
              StandardCopyOption.REPLACE_EXISTING,
              StandardCopyOption.ATOMIC_MOVE
            )
            // Truncate the log and stamp the new journal header at the bumped generation.
            logChannel.truncate(0)
            logChannel.position(0)
            writeJournalHeader(nextGen)
            logChannel.force(true)
            currentGenerationRef.set(nextGen)
            journalBytesSinceCompactionRef.set(0L)
        }
    }

    def close(): Future[Unit] = Future {
        if closed.compareAndSet(false, true) then {
            try tickerHandle.cancel(false)
            catch { case NonFatal(_) => () }
            try scheduler.shutdownNow()
            catch { case NonFatal(_) => () }
            this.synchronized {
                try flushInternal()
                catch { case NonFatal(_) => () }
                try logChannel.close()
                catch { case NonFatal(_) => () }
                try fileLock.release()
                catch { case NonFatal(_) => () }
                try lockChannel.close()
                catch { case NonFatal(_) => () }
            }
        }
    }

    // ------------------------------------------------------------------

    private def readSnapshot(): EngineSnapshotFile = {
        val bytes = Files.readAllBytes(snapshotPath)
        try SchemaMigration.decodeMigrating(bytes)
        catch {
            case e: EnginePersistenceError => throw e
            case NonFatal(t)               => throw EnginePersistenceError.Corrupt(0L, t)
        }
    }

    private def loadJournal(): Seq[JournalRecord] = {
        // Read existing records, truncating at the first malformed tail record. Runs after the
        // constructor has validated/written the header, so bytes 0..JournalHeaderSize is always a
        // valid header matching `currentGeneration`.
        if !Files.exists(logPath) then return Seq.empty
        val bytes = Files.readAllBytes(logPath)
        if bytes.length <= JournalHeaderSize then return Seq.empty
        val builder = Seq.newBuilder[JournalRecord]
        var pos = JournalHeaderSize
        var stopPos = bytes.length
        while pos < bytes.length do {
            if pos + 4 > bytes.length then {
                // Partial length prefix — truncate here.
                stopPos = pos
                pos = bytes.length
            } else {
                val len = ByteBuffer.wrap(bytes, pos, 4).getInt
                if len < 0 || len > bytes.length - pos - 4 then {
                    // Implausible length — treat as corrupt tail, truncate here.
                    System.err.println(
                      s"scalus-stream: truncating corrupt journal tail at byte $pos (len=$len)"
                    )
                    stopPos = pos
                    pos = bytes.length
                } else {
                    val recBytes = java.util.Arrays.copyOfRange(bytes, pos + 4, pos + 4 + len)
                    Try(Cbor.decode(recBytes).to[JournalRecord].value).toOption match {
                        case Some(rec) =>
                            builder += rec
                            pos += 4 + len
                        case None =>
                            System.err.println(
                              s"scalus-stream: truncating malformed journal record at byte $pos"
                            )
                            stopPos = pos
                            pos = bytes.length
                    }
                }
            }
        }
        if stopPos < bytes.length then {
            val trunc = FileChannel.open(logPath, StandardOpenOption.WRITE)
            try
                trunc.truncate(stopPos.toLong)
                trunc.force(true)
            finally trunc.close()
            logChannel.position(stopPos.toLong)
        }
        builder.result()
    }

    /** Validate the journal header against `expectedGen`. If absent / mismatched / pre-v2, discard
      * the journal and write a fresh header. Idempotent — safe to re-run.
      */
    private def initializeJournalHeader(expectedGen: Long): Unit = {
        val size = logChannel.size()
        if size == 0L then {
            logChannel.position(0)
            writeJournalHeader(expectedGen)
            logChannel.force(true)
        } else if size < JournalHeaderSize then {
            warnDiscardingJournal(s"too short to contain a header (size=$size)")
            resetJournal(expectedGen)
        } else {
            readJournalHeader() match {
                case Some(JournalHeader(_, gen)) if gen == expectedGen => ()
                case Some(JournalHeader(_, gen)) =>
                    warnDiscardingJournal(
                      s"generation mismatch (journal=$gen, snapshot=$expectedGen)"
                    )
                    resetJournal(expectedGen)
                case None =>
                    warnDiscardingJournal("no header — legacy v1 journal or corrupt")
                    resetJournal(expectedGen)
            }
        }
    }

    private def resetJournal(generation: Long): Unit = {
        logChannel.truncate(0)
        logChannel.position(0)
        writeJournalHeader(generation)
        logChannel.force(true)
    }

    private def writeJournalHeader(generation: Long): Unit = {
        val hdr = ByteBuffer.allocate(JournalHeaderSize).order(ByteOrder.BIG_ENDIAN)
        hdr.putInt(MagicAsInt)
        hdr.putInt(CurrentJournalSchemaVersion)
        hdr.putLong(generation)
        hdr.flip()
        while hdr.hasRemaining do { val _ = logChannel.write(hdr) }
    }

    private case class JournalHeader(schemaVersion: Int, generation: Long)

    private def readJournalHeader(): Option[JournalHeader] = {
        val savedPos = logChannel.position()
        val buf = ByteBuffer.allocate(JournalHeaderSize).order(ByteOrder.BIG_ENDIAN)
        logChannel.position(0)
        try {
            while buf.hasRemaining do {
                if logChannel.read(buf) < 0 then return None
            }
            buf.flip()
            val magic = buf.getInt
            if magic != MagicAsInt then None
            else {
                val schema = buf.getInt
                val gen = buf.getLong
                Some(JournalHeader(schema, gen))
            }
        } finally logChannel.position(savedPos)
    }

    private def warnDiscardingJournal(reason: String): Unit =
        System.err.println(s"scalus-stream: discarding journal $logPath — $reason")

    private def writeBuffer(): Unit = {
        if buffer.position() == 0 then return
        buffer.flip()
        while buffer.hasRemaining do {
            val _ = logChannel.write(buffer)
        }
        buffer.clear()
    }

    private def writeDirect(bytes: Array[Byte]): Unit = {
        val bb = ByteBuffer.allocate(4 + bytes.length)
        bb.putInt(bytes.length)
        bb.put(bytes)
        bb.flip()
        while bb.hasRemaining do {
            val _ = logChannel.write(bb)
        }
    }

    private def flushInternal(): Unit = this.synchronized {
        if closed.get() then return
        try {
            writeBuffer()
            logChannel.force(false)
        } catch {
            case NonFatal(t) =>
                System.err.println(
                  s"scalus-stream: flush failed on $logPath — ${t.getClass.getSimpleName}: ${t.getMessage}"
                )
                throw EnginePersistenceError.Io(t)
        }
    }
}

object FileEnginePersistenceStore {

    /** Default fsync cadence. Trade-off: shorter → more syscalls, less data lost on SIGKILL; longer
      * → cheaper, up to this much data lost on SIGKILL.
      */
    val DefaultFsyncInterval: FiniteDuration = 1.second

    /** 16 KiB — idle blocks batch naturally; oversized records fall through to direct writes. */
    val DefaultBufferBytes: Int = 16 * 1024

    /** Default journal-size threshold that triggers a runtime compaction (4 MiB). The engine polls
      * [[FileEnginePersistenceStore.compactionDue]] after each `appendSync`; on `true` it builds a
      * snapshot and calls [[FileEnginePersistenceStore.compact]].
      */
    val DefaultCompactionThresholdBytes: Long = 4L * 1024L * 1024L

    /** Fixed-binary journal header — `MAGIC(4) | schemaVersion(u32 BE) | generation(u64 BE)`. The
      * MAGIC's high bit is set so a stray `<len:4>` from a pre-v2 (headerless) journal — which is
      * always non-negative — can never be confused for it.
      */
    val JournalHeaderSize: Int = 16

    /** MAGIC bytes — `0xFF 0xFE 0xFD 0xFC`, deliberately a negative big-endian `Int` so a v1 record
      * length cannot collide with it.
      */
    val MagicBytes: Array[Byte] = Array(0xff.toByte, 0xfe.toByte, 0xfd.toByte, 0xfc.toByte)

    private val MagicAsInt: Int =
        ByteBuffer.wrap(MagicBytes).order(ByteOrder.BIG_ENDIAN).getInt

    /** Bumped on incompatible journal-format changes. v2 is the first version to carry a header. */
    val CurrentJournalSchemaVersion: Int = 2

    def fileForApp(
        appId: String,
        fsyncInterval: FiniteDuration = DefaultFsyncInterval,
        bufferBytes: Int = DefaultBufferBytes,
        compactionThresholdBytes: Long = DefaultCompactionThresholdBytes
    )(using ExecutionContext): EnginePersistenceStore = {
        val dir = PlatformPaths.ensureDir(PlatformPaths.dataDirFor(appId))
        openAt(dir, appId, fsyncInterval, bufferBytes, compactionThresholdBytes)
    }

    def file(
        dir: Path,
        appId: String,
        fsyncInterval: FiniteDuration = DefaultFsyncInterval,
        bufferBytes: Int = DefaultBufferBytes,
        compactionThresholdBytes: Long = DefaultCompactionThresholdBytes
    )(using ExecutionContext): EnginePersistenceStore = {
        Files.createDirectories(dir)
        openAt(dir, appId, fsyncInterval, bufferBytes, compactionThresholdBytes)
    }

    private def openAt(
        dir: Path,
        appId: String,
        fsyncInterval: FiniteDuration,
        bufferBytes: Int,
        compactionThresholdBytes: Long
    )(using ExecutionContext): FileEnginePersistenceStore = {
        require(appId.nonEmpty, "appId must be non-empty")
        new FileEnginePersistenceStore(
          dir,
          appId,
          fsyncInterval,
          bufferBytes,
          compactionThresholdBytes
        )
    }

    /** Delete the persistence files for `appId` in the platform-default directory. Non-fatal if
      * they do not exist. Used by apps that want to explicitly cold-start after catching an
      * [[EnginePersistenceError.Mismatched]] or [[EnginePersistenceError.SchemaMismatch]].
      */
    def reset(appId: String)(using ExecutionContext): Future[Unit] = Future {
        val dir = PlatformPaths.dataDirFor(appId)
        if Files.exists(dir) then {
            Seq(
              dir.resolve(s"$appId.log"),
              dir.resolve(s"$appId.snapshot"),
              dir.resolve(s"$appId.snapshot.tmp"),
              dir.resolve(s"$appId.lock")
            ).foreach(p => Files.deleteIfExists(p))
        }
    }
}
