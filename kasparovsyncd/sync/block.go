package sync

import (
	"github.com/kaspanet/kaspad/app/appmessage"
	"strconv"

	"github.com/kaspanet/kaspad/util/mstime"
	"github.com/kaspanet/kasparov/database"
	"github.com/kaspanet/kasparov/serializer"

	"github.com/kaspanet/kasparov/dbaccess"
	"github.com/kaspanet/kasparov/dbmodels"
	"github.com/pkg/errors"
)

func insertBlocks(dbTx *database.TxContext, blocks []*appmessage.BlockVerboseData, transactionHashesToTxsWithMetadata map[string]*txWithMetadata) error {
	blocksToAdd := make([]interface{}, len(blocks))
	for i, block := range blocks {
		blockMass := uint64(0)
		for _, tx := range block.TransactionVerboseData {
			blockMass += transactionHashesToTxsWithMetadata[tx.Hash].mass
		}
		var err error
		blocksToAdd[i], err = dbBlockFromVerboseBlock(block, blockMass)
		if err != nil {
			return err
		}
	}
	return dbaccess.BulkInsert(dbTx, blocksToAdd)
}

func getBlocksWithTheirParentIDs(dbTx *database.TxContext, blocks []*appmessage.BlockVerboseData) (map[string]uint64, error) {
	blockSet := make(map[string]struct{})
	for _, block := range blocks {
		blockSet[block.Hash] = struct{}{}
		for _, parentHash := range block.ParentHashes {
			blockSet[parentHash] = struct{}{}
		}
	}

	blockHashes := stringsSetToSlice(blockSet)

	dbBlocks, err := dbaccess.BlocksByHashes(dbTx, blockHashes)
	if err != nil {
		return nil, err
	}

	if len(dbBlocks) != len(blockSet) {
		return nil, errors.Errorf("couldn't retrieve all block IDs")
	}

	blockHashesToIDs := make(map[string]uint64)
	for _, dbBlock := range dbBlocks {
		blockHashesToIDs[dbBlock.BlockHash] = dbBlock.ID
	}
	return blockHashesToIDs, nil
}

func dbBlockFromVerboseBlock(verboseBlock *appmessage.BlockVerboseData, mass uint64) (*dbmodels.Block, error) {
	bits, err := strconv.ParseUint(verboseBlock.Bits, 16, 32)
	if err != nil {
		return nil, err
	}

	dbBlock := dbmodels.Block{
		BlockHash:            verboseBlock.Hash,
		Version:              verboseBlock.Version,
		HashMerkleRoot:       verboseBlock.HashMerkleRoot,
		AcceptedIDMerkleRoot: verboseBlock.AcceptedIDMerkleRoot,
		UTXOCommitment:       verboseBlock.UTXOCommitment,
		Timestamp:            mstime.UnixMilliseconds(verboseBlock.Time).ToNativeTime(),
		Bits:                 uint32(bits),
		Nonce:                serializer.Uint64ToBytes(verboseBlock.Nonce),
		BlueScore:            verboseBlock.BlueScore,
		IsChainBlock:         false, // This must be false for updateSelectedParentChain to work properly
		Mass:                 mass,
	}

	// Set genesis block as the initial chain block
	if len(verboseBlock.ParentHashes) == 0 {
		dbBlock.IsChainBlock = true
	}
	return &dbBlock, nil
}
