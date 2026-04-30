#!/usr/bin/env bash
# End-to-end cryptographic verification probe against the real testing-preview
# Mithril aggregator: cert-chain walk → file-level SHA-256 → Cardano-Database
# Merkle root anchored against the certificate's signed_message.
#
# This is the manual probe behind MithrilVerificationProbe — gated on
# SCALUS_MITHRIL_VERIFY_PREVIEW=1 so `sbt test` / CI never invoke it.
#
# Reuses an already-downloaded snapshot at SCALUS_MITHRIL_DEST when one is
# present (resumable via the .extracted markers); otherwise downloads.
#
# Usage:
#   ./scripts/verify_preview.sh                               # temp dir, full download
#   SCALUS_MITHRIL_DEST=/data/preview \
#     ./scripts/verify_preview.sh                             # reuse existing artifact
#   SCALUS_MITHRIL_DEST=/data/preview \
#   SCALUS_MITHRIL_SNAPSHOT_HASH=<hash> \
#     ./scripts/verify_preview.sh                             # pin specific snapshot
#
# A failure here means our MerkleMountainRange port disagrees with upstream
# `ckb-merkle-mountain-range` (or the protocol-message ordering / leaf encoding
# is wrong). The error message includes the recomputed and expected
# signed_message hex so you can compare against the on-wire certificate.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export SCALUS_MITHRIL_VERIFY_PREVIEW=1
export SCALUS_MITHRIL_DEST="${SCALUS_MITHRIL_DEST:-/tmp/mithrill-preview}"

cd "$REPO_ROOT"
exec sbt 'scalusChainStoreMithril/testOnly scalus.cardano.node.stream.engine.snapshot.mithril.MithrilVerificationProbe'
