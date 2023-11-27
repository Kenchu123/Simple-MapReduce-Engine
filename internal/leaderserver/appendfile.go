package leaderserver

import (
	"context"

	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/leaderserver/metadata"
	pb "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/leaderserver/proto"
)

// AppendBlockInfo handles the request to choose the block to append the file
func (l *LeaderServer) AppendBlockInfo(ctx context.Context, in *pb.AppendBlockInfoRequest) (*pb.AppendBlockInfoReply, error) {
	blockInfo, err := l.appendBlockInfo(in.FileName, in.FileSize)
	if err != nil {
		return nil, err
	}
	appendBlockInfoReplyBlockMeta := map[int64]*pb.BlockMeta{}
	for blockID, blockMeta := range blockInfo {
		appendBlockInfoReplyBlockMeta[blockID] = &pb.BlockMeta{
			HostNames: blockMeta.HostNames,
			FileName:  blockMeta.FileName,
			BlockID:   blockMeta.BlockID,
			BlockSize: blockMeta.BlockSize,
		}
	}
	return &pb.AppendBlockInfoReply{
		BlockInfo: appendBlockInfoReplyBlockMeta,
	}, nil
}

// appendBlockInfo select the block to append the file
func (l *LeaderServer) appendBlockInfo(fileName string, fileSize int64) (metadata.BlockInfo, error) {
	oldBlockInfo, err := l.metadata.GetBlockInfo(fileName)
	toAppendBlockInfo := metadata.BlockInfo{}

	if err != nil {
		return nil, err
	}
	// get the last block id
	lastBlockID := int64(-1)
	for blockID := range oldBlockInfo {
		if blockID > lastBlockID {
			lastBlockID = blockID
		}
	}
	// get the last block
	lastBlock := oldBlockInfo[lastBlockID]
	if lastBlock.BlockSize+fileSize <= l.blockSize {
		// append to the last block
		toAppendBlockInfo[lastBlockID] = metadata.BlockMeta{
			HostNames: lastBlock.HostNames,
			FileName:  lastBlock.FileName,
			BlockID:   lastBlock.BlockID,
			BlockSize: lastBlock.BlockSize, // should be updated by client after put
		}
		return toAppendBlockInfo, nil
	}

	// create new blocks
	if l.blockSize-lastBlock.BlockSize > 0 {
		toAppendBlockInfo[lastBlockID] = metadata.BlockMeta{
			HostNames: lastBlock.HostNames,
			FileName:  lastBlock.FileName,
			BlockID:   lastBlock.BlockID,
			BlockSize: lastBlock.BlockSize, // should be updated by client after put
		}
	}
	leftBlockSize := fileSize - (l.blockSize - lastBlock.BlockSize)
	blocksNum := leftBlockSize / l.blockSize
	if leftBlockSize%l.blockSize != 0 {
		blocksNum++
	}
	for i := int64(0); i < blocksNum; i++ {
		toAppendBlockInfo[lastBlockID+i+1] = metadata.BlockMeta{
			HostNames: l.selectBlockHosts(),
			FileName:  fileName,
			BlockID:   lastBlockID + i + 1,
			BlockSize: 0, // should be updated by client after put
		}
	}
	return toAppendBlockInfo, nil
}

func (l *LeaderServer) AppendFileOK(ctx context.Context, in *pb.AppendFileOKRequest) (*pb.AppendFileOKReply, error) {
	for _, blockMeta := range in.BlockInfo {
		newBlockMeta := metadata.BlockMeta{
			HostNames: blockMeta.HostNames,
			FileName:  blockMeta.FileName,
			BlockID:   blockMeta.BlockID,
			BlockSize: blockMeta.BlockSize,
		}
		l.metadata.AddOrUpdateBlockMeta(in.FileName, newBlockMeta)

	}
	return &pb.AppendFileOKReply{}, nil
}
