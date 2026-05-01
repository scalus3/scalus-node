package scalus.cardano.node.stream.engine.snapshot.mithril

import com.dylibso.chicory.wasm.Parser
import com.dylibso.chicory.wasm.types.{ExternalType, OpCode}
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters.*

/** Maps the hot function indices the call-profiler identified in the release WASM blob to
  * their (1) called-imports and (2) likely names by structural matching against the
  * locally-built dev WASM. The release blob has no `name` section; the dev blob does.
  *
  * Approach for cross-blob name lookup: for each release fn we summarise its
  * `(call-target-import-set, num-instructions, num-CALLs)` signature. Then we walk every
  * named function in the dev blob and find ones with matching signature. That doesn't always
  * yield a unique match (different bodies can have the same signature) but for hot internals
  * it usually narrows to a handful. Pair with the import-set print, you can pick the right
  * one by hand.
  *
  * Run: `sbt 'scalusChainStoreMithril/testOnly *HotFunctionAnalysisProbe'`. Always-on, no
  * env gate — the blobs are on the classpath / fixed local path.
  */
final class HotFunctionAnalysisProbe extends AnyFunSuite {

    test("name the hot release functions via the dev blob's name section") {
        val releasePath = "scalus-chain-store-mithril/src/test/resources/mithril/mithril_client_wasm_bg.wasm"
        val devPath = "/tmp/mithril-dev/mithril_client_wasm_bg.wasm"
        if !java.nio.file.Files.exists(java.nio.file.Paths.get(devPath)) then {
            cancel(s"dev WASM missing at $devPath — build with `cargo build --target wasm32-unknown-unknown` and `wasm-bindgen ...`")
        }

        val release = Parser.parse(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(releasePath)))
        val dev = Parser.parse(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(devPath)))

        val hotFns = Seq(1482, 387, 469, 136, 443, 2630, 2680, 748, 327, 246, 868)

        // ---- Release blob: imports + call-target dump for hot functions
        val rImports = release.importSection().stream().iterator().asScala
            .filter(_.importType() == ExternalType.FUNCTION).toIndexedSeq
        val rImportNames = rImports.map(_.name())
        val nrImports = rImportNames.size
        val rCode = release.codeSection()

        def releaseSig(globalIdx: Int): Option[(Set[String], Int, Int)] = {
            if globalIdx < nrImports then None
            else {
                val body = rCode.getFunctionBody(globalIdx - nrImports)
                val instrs = body.instructions()
                var i = 0
                var nCalls = 0
                val imports = scala.collection.mutable.Set.empty[String]
                while i < instrs.size do {
                    val op = instrs.get(i).opcode()
                    if op == OpCode.CALL then {
                        nCalls += 1
                        val target = instrs.get(i).operand(0).toInt
                        if target < nrImports then imports += rImportNames(target)
                    }
                    i += 1
                }
                Some((imports.toSet, instrs.size, nCalls))
            }
        }

        info("=== Release blob: imports called by hot fns")
        hotFns.foreach { idx =>
            releaseSig(idx) match {
                case None => info(s"  fn#$idx: <import or out-of-range>")
                case Some((imports, instrCount, nCalls)) =>
                    info(s"  fn#$idx: instrs=$instrCount calls=$nCalls importsCalled=${imports.size}")
                    val sample = imports.toSeq.sorted.take(8)
                    sample.foreach(n => info(s"      $n"))
            }
        }

        // ---- Dev blob: build (set-of-imports, instrCount, callCount) -> [name] index
        val devImports = dev.importSection().stream().iterator().asScala
            .filter(_.importType() == ExternalType.FUNCTION).toIndexedSeq
        val devImportNames = devImports.map(_.name())
        val ndImports = devImportNames.size
        val dCode = dev.codeSection()
        val devNames = dev.nameSection()

        info(s"=== Dev blob: ${dev.functionSection().functionCount()} local fns, name-section: ${devNames != null}")
        if devNames == null then cancel("dev wasm has no name section — unexpected")

        // Map (signature) -> List[devFnIdx]
        val devSigIndex = scala.collection.mutable.Map.empty[(Set[String], Int, Int), List[Int]]
        val ndLocals = dev.functionSection().functionCount()
        var d = 0
        while d < ndLocals do {
            val global = ndImports + d
            val body = dCode.getFunctionBody(d)
            val instrs = body.instructions()
            var i = 0
            var nCalls = 0
            val imports = scala.collection.mutable.Set.empty[String]
            while i < instrs.size do {
                val op = instrs.get(i).opcode()
                if op == OpCode.CALL then {
                    nCalls += 1
                    val tgt = instrs.get(i).operand(0).toInt
                    if tgt < ndImports then imports += devImportNames(tgt)
                }
                i += 1
            }
            val key = (imports.toSet, instrs.size, nCalls)
            devSigIndex.updateWith(key) {
                case Some(xs) => Some(global :: xs)
                case None     => Some(global :: Nil)
            }
            d += 1
        }

        info("=== Cross-blob match candidates by (import-set, instrCount, callCount)")
        hotFns.foreach { idx =>
            releaseSig(idx) match {
                case None => ()
                case Some(sig) =>
                    val candidates = devSigIndex.getOrElse(sig, Nil)
                    info(s"  release fn#$idx (instrs=${sig._2} calls=${sig._3} imports=${sig._1.size})")
                    info(s"    -> ${candidates.size} dev candidate(s)")
                    candidates.take(5).foreach { gIdx =>
                        val name = devNames.nameOfFunction(gIdx)
                        info(s"       fn#$gIdx  $name")
                    }
            }
        }
    }
}
