package scalus.cardano.network.n2c

import cps.*
import cps.monads.FutureAsyncMonad
import io.bullet.borer.Cbor
import scalus.cardano.infra.{CancelSource, CancelToken, CancelledException}
import scalus.cardano.ledger.{BlockHash, KeepRaw, OriginalCborByteArray}
import scalus.cardano.network.chainsync.{ChainSyncDriver, ChainSyncEvent, IntersectSeeker, Point, Tip}
import scalus.cardano.network.infra.MiniProtocolId
import scalus.cardano.network.{BlockEnvelope, Era}
import scalus.cardano.node.stream.engine.{AppliedBlock, Engine}
import scalus.cardano.node.stream.{ChainPoint, ChainTip, StartFrom}
import scalus.uplc.builtin.{platform, ByteString}

import scala.concurrent.{ExecutionContext, Future}

/** N2C analogue of [[scalus.cardano.network.ChainApplier]]. Drives `LocalChainSync` over a live
  * [[NodeToClientConnection]] and forwards each event into the [[Engine]].
  *
  * Differences from N2N's `ChainApplier`:
  *
  *   - Uses [[MiniProtocolId.LocalChainSync]] (id 5) instead of [[MiniProtocolId.ChainSync]] (id
  *     2).
  *   - There is no BlockFetch on N2C — `MsgRollForward` ships the **full era-specific Block** as
  *     its payload, so we decode the block inline via [[BlockEnvelope.decodeBlock]] and skip the
  *     N2N's two-step header-then-body dance entirely.
  *   - The `ChainPoint` for an observed Forward is built from the parsed block header. We re-encode
  *     the header to derive the wire-canonical header CBOR; if scalus's encoder ever diverges from
  *     the cardano-node encoder for a given era, the resulting point hash would drift from what the
  *     peer reports on `MsgRollBackward` and rollback would miss. The `YaciN2cChainSyncSuite` IT
  *     (and the loopback responder) is the canary on this.
  *
  * Cancellation, back-pressure, and lifecycle mirror the N2N applier — the same `applierScope`
  * convention applies, the `done` future captures the loop's terminal state, and `close()` on the
  * driver runs best-effort during teardown.
  */
private final class N2cChainApplier(
    conn: NodeToClientConnection,
    engine: Engine,
    cancelToken: CancelToken,
    logger: scribe.Logger
)(using ExecutionContext) {

    private val chainSync = new ChainSyncDriver(
      conn.channel(MiniProtocolId.LocalChainSync),
      cancelToken,
      logger
    )

    def run(startFrom: StartFrom): Future[Unit] = async[Future] {
        try {
            val intersect = await(IntersectSeeker.seek(chainSync, startFrom))
            logger.info(s"n2c chain-sync intersected at $intersect; starting loop")
            await(loop())
        } finally chainSync.close()
    }

    private def loop(): Future[Unit] = async[Future] {
        var running = true
        while running do {
            await(chainSync.next()) match {
                case None =>
                    logger.info("peer sent MsgDone; n2c chain-sync loop complete")
                    running = false
                case Some(ChainSyncEvent.Forward(era, blockBytes, tip)) =>
                    await(processForward(era, blockBytes, tip))
                case Some(ChainSyncEvent.Backward(to, _)) =>
                    // Same first-rollback-after-intersect convention as the N2N applier:
                    // ouroboros-network always echoes the resume point back as RollBackward,
                    // not a real rewind. Treat it as a no-op when the engine has no tip yet.
                    if engine.currentTip.isEmpty then
                        logger.info(
                          s"ignoring initial RollBackward to ${Point.toChainPoint(to)} " +
                              s"(engine has no tip yet; protocol confirmation of intersect)"
                        )
                    else {
                        val point = Point.toChainPoint(to)
                        logger.debug(s"RollBackward to $point")
                        await(engine.onRollBackward(point))
                    }
            }
        }
    }

    private def processForward(
        era: Int,
        blockBytes: ByteString,
        peerTip: Tip
    ): Future[Unit] = async[Future] {
        val blockRaw: KeepRaw[?] = BlockEnvelope.decodeBlock(Era.fromWire(era), blockBytes) match {
            case Left(err) => throw err
            case Right(b)  => b
        }
        // Re-encode the header to derive the wire-canonical bytes for the point hash. See the
        // class scaladoc on the canary.
        val block = blockRaw.value.asInstanceOf[scalus.cardano.ledger.Block]
        val headerBytes = ByteString.fromArray(Cbor.encode(block.header).toByteArray)
        val hash = BlockHash.fromByteString(platform.blake2b_256(headerBytes))
        val point = ChainPoint(block.header.slot, hash)
        val tip = ChainTip(point, block.header.blockNumber)

        logger.debug(s"RollForward block @ $point (peer tip $peerTip)")

        // OriginalCborByteArray is already set up by BlockEnvelope.decodeBlock — reuse the
        // same projection helper as the N2N applier so both paths produce identical
        // AppliedBlock shapes.
        given OriginalCborByteArray = OriginalCborByteArray(blockBytes.bytes)
        val applied = AppliedBlock.fromRaw(
          tip,
          blockRaw.asInstanceOf[KeepRaw[scalus.cardano.ledger.Block]]
        )
        await(engine.onRollForward(applied))
    }
}

/** Handle returned by [[N2cChainApplier.spawn]]. Same lifecycle contract as the N2N applier handle
  * — cancel/done semantics line up so the provider's teardown logic is symmetric.
  */
final class N2cChainApplierHandle private[n2c] (
    applierScope: CancelSource,
    val done: Future[Unit]
)(using ExecutionContext) {

    def cancel(cause: Throwable = new CancelledException("n2c applier cancel")): Future[Unit] = {
        if !applierScope.token.isCancelled then applierScope.cancel(cause)
        done.recover { case _ => () }
    }
}

object N2cChainApplier {

    private val defaultLogger: scribe.Logger =
        scribe.Logger("scalus.cardano.network.n2c.N2cChainApplier")

    def spawn(
        conn: NodeToClientConnection,
        engine: Engine,
        startFrom: StartFrom,
        logger: scribe.Logger = defaultLogger
    )(using ExecutionContext): N2cChainApplierHandle = {
        val applierScope = CancelSource.linkedTo(conn.rootToken)
        val applier = new N2cChainApplier(conn, engine, applierScope.token, logger)
        val done = applier.run(startFrom)
        new N2cChainApplierHandle(applierScope, done)
    }
}
