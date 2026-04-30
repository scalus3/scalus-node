package scalus.cardano.node.stream.engine.snapshot.mithril

/** Mirror of upstream `mithril-common::entities::ProtocolMessagePartKey`. Declaration order is
  * load-bearing — it dictates the iteration order of [[ProtocolMessageHash.compute]]. Adding a
  * key upstream means appending a case here; reordering breaks every signature comparison.
  */
enum ProtocolMessagePartKey(val snakeCase: String) {
    case SnapshotDigest extends ProtocolMessagePartKey("snapshot_digest")
    case CardanoTransactionsMerkleRoot
        extends ProtocolMessagePartKey("cardano_transactions_merkle_root")
    case CardanoBlocksTransactionsMerkleRoot
        extends ProtocolMessagePartKey("cardano_blocks_transactions_merkle_root")
    case NextAggregateVerificationKey
        extends ProtocolMessagePartKey("next_aggregate_verification_key")
    case NextProtocolParameters extends ProtocolMessagePartKey("next_protocol_parameters")
    case CurrentEpoch extends ProtocolMessagePartKey("current_epoch")
    case LatestBlockNumber extends ProtocolMessagePartKey("latest_block_number")
    case CardanoBlocksTransactionsBlockNumberOffset
        extends ProtocolMessagePartKey("cardano_blocks_transactions_block_number_offset")
    case CardanoStakeDistributionEpoch
        extends ProtocolMessagePartKey("cardano_stake_distribution_epoch")
    case CardanoStakeDistributionMerkleRoot
        extends ProtocolMessagePartKey("cardano_stake_distribution_merkle_root")
    case CardanoDatabaseMerkleRoot extends ProtocolMessagePartKey("cardano_database_merkle_root")
    case NextSnarkAggregateVerificationKey
        extends ProtocolMessagePartKey("next_aggregate_verification_key_snark")
}

object ProtocolMessagePartKey {
    private val byName: Map[String, ProtocolMessagePartKey] =
        values.iterator.map(k => k.snakeCase -> k).toMap

    def fromSnakeCase(s: String): Option[ProtocolMessagePartKey] = byName.get(s)
}
