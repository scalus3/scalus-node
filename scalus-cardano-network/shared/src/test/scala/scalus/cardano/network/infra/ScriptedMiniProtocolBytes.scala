package scalus.cardano.network.infra

import io.bullet.borer.Encoder
import scalus.cardano.infra.{CancelToken, CancelledException}
import scalus.serialization.cbor.Cbor
import scalus.uplc.builtin.ByteString

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Test double for [[MiniProtocolBytes]]: a scripted byte channel for driving mini-protocol state
  * machines in unit tests. Stage server replies with [[stage]] (CBOR-encoded via the scalus codec,
  * which tolerates the raw-bytes splice some N2C message codecs use); inspect what the driver wrote
  * via [[sentOutbound]]. `receive` parks until a reply is staged.
  *
  * Generic over the mini-protocol's message type — `LocalStateQueryDriverSuite` and
  * `LocalTxMonitorDriverSuite` share one instance shape.
  *
  * Cancel observance: a parked `receive` fails with [[CancelledException]] when its `CancelToken`
  * fires — same shape as the production socket-backed `MiniProtocolBytes`. Tests that pass
  * `CancelToken.never` (or omit the argument) get the pre-existing no-cancel behaviour for free.
  */
final class ScriptedMiniProtocolBytes[M: Encoder] extends MiniProtocolBytes {
    private val lock = new AnyRef
    private val inbound = mutable.ArrayDeque.empty[Option[ByteString]]
    private var pending: Option[Promise[Option[ByteString]]] = None

    /** Every message the driver has sent, in order. */
    val sentOutbound: mutable.ArrayBuffer[ByteString] = mutable.ArrayBuffer.empty

    def receive(cancel: CancelToken = CancelToken.never): Future[Option[ByteString]] =
        lock.synchronized {
            if inbound.nonEmpty then Future.successful(inbound.removeHead())
            else {
                val p = Promise[Option[ByteString]]()
                pending = Some(p)
                // Observe cancel so a parked receive aborts when the token fires — matches the
                // real socket-backed MiniProtocolBytes. `CancelToken.never.onCancel` is a no-op,
                // so callers that pass it (the default) are unaffected.
                val reg = cancel.onCancel { () =>
                    val _ = p.tryFailure(CancelledException("receive cancelled"))
                    lock.synchronized {
                        if pending.contains(p) then pending = None
                    }
                }
                // Deregister the listener when the Promise completes via `stage(...)` so a
                // long-lived cancel source doesn't accumulate stale closures.
                p.future.onComplete(_ => reg.cancel())(ExecutionContext.parasitic)
                p.future
            }
        }

    def send(message: ByteString, cancel: CancelToken = CancelToken.never): Future[Unit] =
        lock.synchronized {
            sentOutbound += message
            Future.unit
        }

    /** Queue a server reply — delivered to a parked `receive`, or buffered for the next one. */
    def stage(reply: M): Unit = lock.synchronized {
        val payload = Some(ByteString.unsafeFromArray(Cbor.encode(reply)))
        pending match {
            case Some(p) =>
                pending = None
                val _ = p.trySuccess(payload)
            case None =>
                inbound.append(payload)
        }
    }
}
