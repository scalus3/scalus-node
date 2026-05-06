package scalus.cardano.node.stream

import scalus.cardano.ledger.TransactionHash

/** Read-only introspection on a configured `BackupSource` implementation.
  *
  * Implementations of `BlockchainProvider` that back a `BackupSource` MAY mix this in to expose
  * deployment-time diagnostics (connection age, submit counters, last-submitted hash). The
  * streaming provider's `backupDiagnostics` returns `Some(...)` when the configured backup
  * implements the trait, `None` otherwise — callers that need universal diagnostics decide locally
  * how to render the absence.
  *
  * Designed for ops dashboards and one-shot health checks. Not a metrics-system bridge —
  * application code wanting Prometheus/OpenTelemetry pulls these counters and exports them in its
  * own format.
  */
trait BackupDiagnostics {

    /** Atomic snapshot of every diagnostic field. Single-method API so consumers see a coherent
      * point-in-time view across counters that mutate independently.
      */
    def diagnostics: BackupDiagnosticsSnapshot
}

/** Point-in-time snapshot of a [[BackupDiagnostics]] reading.
  *
  * @param connectedSinceMillis
  *   `System.currentTimeMillis()` at which the underlying connection was established. `0L` for
  *   backups that never establish a long-lived connection (e.g. HTTPS-style Blockfrost).
  * @param lastSubmittedHash
  *   most recently *successful* submit's [[TransactionHash]]. `None` if no successful submit yet.
  * @param submitCount
  *   total submit attempts since the backup connected — both successful and rejected.
  * @param rejectCount
  *   subset of [[submitCount]] that surfaced a typed rejection from the node. Network/transport
  *   failures are not counted here; they propagate as future failures, not `Left(SubmitError)`.
  */
final case class BackupDiagnosticsSnapshot(
    connectedSinceMillis: Long,
    lastSubmittedHash: Option[TransactionHash],
    submitCount: Long,
    rejectCount: Long
)
