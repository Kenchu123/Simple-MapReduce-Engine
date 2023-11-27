package leaderserver

import (
	"context"
	"fmt"

	pb "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/leaderserver/proto"

	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/leaderserver/metadata"
)

func (l *LeaderServer) GetBlockInfo(ctx context.Context, in *pb.GetBlockInfoRequest) (*pb.GetBlockInfoReply, error) {
	blockInfo, err := l.getBlockInfo(in.FileName)
	if err != nil {
		return nil, err
	}
	getBlockInfoReplyBlockMeta := map[int64]*pb.BlockMeta{}
	for blockID, blockMeta := range blockInfo {
		getBlockInfoReplyBlockMeta[blockID] = &pb.BlockMeta{
			HostNames: blockMeta.HostNames,
			FileName:  blockMeta.FileName,
			BlockID:   blockMeta.BlockID,
			BlockSize: blockMeta.BlockSize,
		}
	}
	return &pb.GetBlockInfoReply{
		BlockInfo: getBlockInfoReplyBlockMeta,
	}, nil
}

func (l *LeaderServer) getBlockInfo(fileName string) (metadata.BlockInfo, error) {
	return l.metadata.GetBlockInfo(fileName)
}

func (l *LeaderServer) GetFileOK(ctx context.Context, in *pb.GetFileOKRequest) (*pb.GetFileOKReply, error) {
	if l.metadata.IsFileExist(in.FileName) == false {
		return nil, fmt.Errorf("file %s not found", in.FileName)
	}
	return &pb.GetFileOKReply{}, nil
}
