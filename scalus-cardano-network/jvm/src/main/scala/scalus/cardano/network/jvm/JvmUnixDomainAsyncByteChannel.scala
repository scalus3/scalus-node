package scalus.cardano.network.jvm

import scalus.cardano.infra.{CancelToken, CancelledException}
import scalus.cardano.network.infra.AsyncByteChannel
import scalus.uplc.builtin.ByteString

import java.io.IOException
import java.net.{StandardProtocolFamily, UnixDomainSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousCloseException, ClosedChannelException, SocketChannel}
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, ThreadFactory}
import scala.concurrent.{blocking, ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

/** JVM [[AsyncByteChannel]] over a blocking Unix-domain `SocketChannel`. The JDK's
  * `AsynchronousSocketChannel` family does **not** support `StandardProtocolFamily.UNIX` (only INET
  * / INET6), so the N2C transport sits on a blocking [[SocketChannel]] and uses a small dedicated
  * executor to keep the `AsyncByteChannel` contract honest.
  *
  * Threading model: one single-threaded executor per channel — we serialise read syscalls behind
  * one thread and write syscalls behind another. The mux contract permits one read + one write in
  * flight at a time (see [[AsyncByteChannel]]'s scaladoc), and one Unix-domain connection per
  * process is the typical case for N2C. Wrapping both directions on a single thread would deadlock
  * the writer behind a long-running blocking read, so we use two.
  *
  * Cancellation: a cancel observed pre-syscall fails the future immediately. A cancel observed
  * mid-syscall has no effect on its own — the only safe abort while a `read`/`write` is parked
  * inside the kernel is to close the channel, which raises [[AsynchronousCloseException]]. The
  * caller therefore cancels via [[close]] for that case; partial writes are not retried (the same
  * "no half-frame" rule the NIO2 N2N path applies).
  */
final class JvmUnixDomainAsyncByteChannel private (
    ch: SocketChannel,
    refillSize: Int,
    readEc: ExecutionContext,
    writeEc: ExecutionContext,
    onClose: () => Unit
) extends AsyncByteChannel {

    // Inbound refill buffer. Same invariants as JvmAsyncByteChannel: in *read mode* on
    // entry/exit (position..limit covers buffered-but-undelivered bytes). Initialised empty.
    private val refill = ByteBuffer.allocate(refillSize)
    refill.position(0); refill.limit(0)

    private val closedFlag = new AtomicBoolean(false)

    def readExactly(n: Int, cancel: CancelToken): Future[Option[ByteString]] = {
        require(n >= 0, "n must be non-negative")
        if n == 0 then Future.successful(Some(ByteString.empty))
        else if closedFlag.get then Future.failed(closedExc())
        else if cancel.isCancelled then Future.failed(CancelledException("pre-read"))
        else {
            val out = new Array[Byte](n)
            Future {
                blocking {
                    readLoopBlocking(out, 0, cancel)
                }
            }(readEc)
        }
    }

    private def readLoopBlocking(
        out: Array[Byte],
        filledIn: Int,
        cancel: CancelToken
    ): Option[ByteString] = {
        var filled = filledIn
        while filled < out.length do {
            if cancel.isCancelled then
                throw CancelledException(s"after $filled/${out.length} bytes")
            if !refill.hasRemaining then {
                refill.clear()
                val read =
                    try ch.read(refill)
                    catch {
                        case _: AsynchronousCloseException | _: ClosedChannelException =>
                            throw closedExc()
                    }
                refill.flip()
                if read < 0 then {
                    if filled == 0 then return None
                    else throw new AsyncByteChannel.UnexpectedEofException(out.length, filled)
                }
            }
            val take = math.min(out.length - filled, refill.remaining())
            refill.get(out, filled, take)
            filled += take
        }
        Some(ByteString.unsafeFromArray(out))
    }

    def write(bytes: ByteString, cancel: CancelToken): Future[Unit] = {
        if closedFlag.get then Future.failed(closedExc())
        else if cancel.isCancelled then Future.failed(CancelledException("pre-write"))
        else if bytes.bytes.length == 0 then Future.unit
        else {
            val buf = ByteBuffer.wrap(bytes.bytes)
            Future {
                blocking {
                    while buf.hasRemaining do {
                        try {
                            val _ = ch.write(buf)
                        } catch {
                            case _: AsynchronousCloseException | _: ClosedChannelException =>
                                throw closedExc()
                        }
                    }
                }
            }(writeEc)
        }
    }

    def close(): Future[Unit] = {
        if closedFlag.compareAndSet(false, true) then {
            try {
                ch.close()
                onClose()
                Future.unit
            } catch {
                case NonFatal(t) => Future.failed(t)
            }
        } else Future.unit
    }

    private def closedExc(): AsyncByteChannel.ChannelClosedException =
        new AsyncByteChannel.ChannelClosedException("channel closed")
}

object JvmUnixDomainAsyncByteChannel {

    /** Default refill-buffer size: 64 KiB. Same rationale as the TCP variant. */
    val DefaultRefillSize: Int = 64 * 1024

    private val threadCounter = new java.util.concurrent.atomic.AtomicLong()

    private def daemonFactory(name: String): ThreadFactory = (r: Runnable) => {
        val t = new Thread(r, s"$name-${threadCounter.incrementAndGet()}")
        t.setDaemon(true)
        t
    }

    /** Open a Unix-domain socket connection at `path` and wrap it. Connect is itself a blocking
      * call so we run it on the supplied EC's thread; the returned future completes once the peer
      * accept has been observed.
      */
    def connect(
        path: Path,
        refillSize: Int = DefaultRefillSize
    )(using ec: ExecutionContext): Future[JvmUnixDomainAsyncByteChannel] = {
        val p = Promise[JvmUnixDomainAsyncByteChannel]()
        ec.execute(() => {
            try {
                val sc = SocketChannel.open(StandardProtocolFamily.UNIX)
                try sc.connect(UnixDomainSocketAddress.of(path))
                catch {
                    case NonFatal(t) =>
                        try sc.close()
                        catch { case NonFatal(c) => t.addSuppressed(c) }
                        throw t
                }
                p.success(fromChannel(sc, refillSize))
            } catch {
                case NonFatal(t) =>
                    val _ = p.tryFailure(t)
                case t: IOException =>
                    val _ = p.tryFailure(t)
            }
        })
        p.future
    }

    /** Wrap an already-connected blocking `SocketChannel`. Used for testing with a Unix-domain
      * socket pair. The returned channel owns the supplied `SocketChannel` and will close it on
      * [[JvmUnixDomainAsyncByteChannel.close]].
      */
    def fromChannel(
        ch: SocketChannel,
        refillSize: Int = DefaultRefillSize
    ): JvmUnixDomainAsyncByteChannel = {
        require(ch.isBlocking, "channel must be in blocking mode")
        val readExec = Executors.newSingleThreadExecutor(daemonFactory("scalus-n2c-read"))
        val writeExec = Executors.newSingleThreadExecutor(daemonFactory("scalus-n2c-write"))
        val readEc = ExecutionContext.fromExecutorService(readExec)
        val writeEc = ExecutionContext.fromExecutorService(writeExec)
        new JvmUnixDomainAsyncByteChannel(
          ch,
          refillSize,
          readEc,
          writeEc,
          onClose = () => {
              readExec.shutdown()
              writeExec.shutdown()
          }
        )
    }
}
