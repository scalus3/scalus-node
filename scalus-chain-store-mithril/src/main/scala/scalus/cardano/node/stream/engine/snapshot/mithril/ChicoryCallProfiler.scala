package scalus.cardano.node.stream.engine.snapshot.mithril

import com.dylibso.chicory.runtime.{ExecutionListener, MStack}
import com.dylibso.chicory.wasm.WasmModule
import com.dylibso.chicory.wasm.types.{Instruction, NameCustomSection, OpCode}

import java.io.{FileWriter, PrintWriter}
import java.util.concurrent.atomic.{AtomicLong, AtomicLongArray}
import java.util.concurrent.{Executors, ThreadFactory, TimeUnit}

/** Per-instruction sampling listener that counts WASM `CALL` events by target function index,
  * and periodically dumps a delta-rate top-K to stderr.
  *
  * Resolution: function indices are mapped to Rust-mangled names via the embedded `name` custom
  * section (wasm-bindgen always emits one). Indices for which no name exists fall back to
  * `fn#<idx>`.
  *
  * Trade-offs:
  *   - The listener fires on EVERY instruction, then short-circuits unless the opcode is a CALL.
  *     Per-instruction overhead is real but bearable for diagnostic runs (we already pay 100% CPU
  *     on the interpreter; a few ns extra per opcode doesn't change behaviour, just slows it
  *     uniformly).
  *   - `CALL_INDIRECT` isn't counted by target — the destination depends on the value stack which
  *     we don't track here. Counts by *type-index* would be misleading. Practical impact: closures
  *     invoked via the function table show up at the JS-glue caller side rather than at the
  *     ultimate target, but that's still useful for narrowing down hot regions.
  *   - We periodically print *deltas* since the previous dump, so a busy-loop manifests as the
  *     same function dominating each window.
  */
final class ChicoryCallProfiler(
    module: WasmModule,
    dumpEverySec: Long = 10L,
    topK: Int = 20,
    outputPath: String = "/tmp/mithril-call-profile.log"
) extends ExecutionListener {

    private val out = new PrintWriter(new FileWriter(outputPath, /*append=*/ false), /*autoFlush=*/ true)
    out.println(s"# ChicoryCallProfiler started at ${java.time.Instant.now} (dump=${dumpEverySec}s top=$topK)")
    out.flush()

    private val nameSection: NameCustomSection = module.nameSection()
    private val totalFns: Int = {
        val imports = module.importSection().count(com.dylibso.chicory.wasm.types.ExternalType.FUNCTION)
        val locals = module.functionSection().functionCount()
        // Imports come first in the function index space, then defined functions.
        imports + locals
    }
    private val callCounts: AtomicLongArray = new AtomicLongArray(totalFns + 1)
    private val totalCalls: AtomicLong = new AtomicLong(0L)
    private val startNanos: Long = System.nanoTime()

    private val dumper = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
        def newThread(r: Runnable): Thread = {
            val t = new Thread(r, "mithril-call-profiler")
            t.setDaemon(true)
            t
        }
    })

    private val previousSnapshot: Array[Long] = new Array[Long](totalFns + 1)
    @volatile private var prevTotal: Long = 0L
    @volatile private var prevWallNanos: Long = startNanos

    dumper.scheduleAtFixedRate(
      new Runnable {
          def run(): Unit =
              try dump()
              catch {
                  case scala.util.control.NonFatal(t) =>
                      out.println(
                        s"# profiler dump threw ${t.getClass.getSimpleName}: ${t.getMessage}"
                      )
                      t.getStackTrace.take(8).foreach(s => out.println(s"#   $s"))
                      out.flush()
              }
      },
      dumpEverySec,
      dumpEverySec,
      TimeUnit.SECONDS
    )

    def onExecution(instr: Instruction, stack: MStack): Unit = {
        if instr.opcode() == OpCode.CALL && instr.operandCount() >= 1 then {
            val fnIdx = instr.operand(0).toInt
            if fnIdx >= 0 && fnIdx <= totalFns then {
                callCounts.incrementAndGet(fnIdx)
                totalCalls.incrementAndGet()
            }
        }
    }

    private def dump(): Unit = {
        val now = System.nanoTime()
        val elapsedSec = (now - startNanos) / 1_000_000_000L
        val windowSec = (now - prevWallNanos) / 1_000_000_000.0
        val total = totalCalls.get
        val totalDelta = total - prevTotal
        prevTotal = total
        prevWallNanos = now

        val deltas: Array[(Int, Long)] = (0 to totalFns).iterator.map { idx =>
            val cur = callCounts.get(idx)
            val prev = previousSnapshot(idx)
            previousSnapshot(idx) = cur
            (idx, cur - prev)
        }.toArray

        val top = deltas.filter(_._2 > 0).sortBy(-_._2).take(topK)
        val rate = if windowSec > 0.0 then (totalDelta.toDouble / windowSec).toLong else 0L
        val header = f"[chicory-prof @ ${elapsedSec}%5ds] " +
            f"window=${windowSec}%4.1fs calls=$totalDelta%-10d rate=$rate%-10d/s top$topK:"
        val body = top
            .map { (idx, delta) =>
                val pct = if totalDelta > 0 then 100.0 * delta / totalDelta else 0.0
                f"  $idx%6d  $delta%10d  ${pct}%5.1f%%  ${nameOf(idx)}"
            }
            .mkString("\n")
        out.println(header)
        if body.nonEmpty then out.println(body)
        out.flush()
    }

    private def nameOf(idx: Int): String = {
        val n =
            if nameSection == null then null
            else
                try nameSection.nameOfFunction(idx)
                catch { case scala.util.control.NonFatal(_) => null }
        if n == null || n.isEmpty then s"fn#$idx" else n
    }

    /** Stop the dumper thread. Idempotent. */
    def close(): Unit = {
        dumper.shutdown()
        out.flush()
        out.close()
    }
}
