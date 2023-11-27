package client

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"time"

	leaderServerProto "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/leaderserver/proto"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/sirupsen/logrus"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/leaderserver/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (c *Client) AppendFile(localfilename, sdfsfilename string) error {
	localfile, err := os.Open(localfilename)
	if err != nil {
		return fmt.Errorf("cannot open local file %s: %v", localfilename, err)
	}
	defer localfile.Close()
	fileInfo, err := localfile.Stat()
	if err != nil {
		return fmt.Errorf("cannot get local file %s info: %v", localfilename, err)
	}

	// get leader, ask leader where to store the file, send the file to the data server
	leader, err := c.getLeader()
	if err != nil {
		return err
	}
	logrus.Infof("Leader is %s", leader)

	// acquire write lock
	err = c.acquireFileWriteLock(leader, sdfsfilename)
	if err != nil {
		return err
	}
	defer c.releaseFileWriteLock(leader, sdfsfilename)
	logrus.Infof("Acquired write lock of file %s", sdfsfilename)

	blockInfo, err := c.appendBlockInfo(leader, sdfsfilename, fileInfo.Size())
	if err != nil {
		return err
	}
	logrus.Infof("Got blockInfo %+v", blockInfo)

	// get the blockIDs
	blockIDs := []int64{}
	for blockID := range blockInfo {
		blockIDs = append(blockIDs, blockID)
		// Fetch the last block which is not full
	}
	// sort the blockIDs
	sort.Slice(blockIDs, func(i, j int) bool {
		return blockIDs[i] < blockIDs[j]
	})

	// get the first block if the first block is not empty
	firstBlock := blockInfo[blockIDs[0]]
	firstBlockData := make([]byte, firstBlock.BlockSize)
	if firstBlock.BlockSize != 0 {
		eg, _ := errgroup.WithContext(context.Background())
		func(blockMeta metadata.BlockMeta) {
			eg.Go(func() error {
				hostNames := blockMeta.HostNames
				rand.Shuffle(len(hostNames), func(i, j int) {
					hostNames[i], hostNames[j] = hostNames[j], hostNames[i]
				})
				// get the first block file from multiple servers concurrently
				for _, hostName := range hostNames {
					logrus.Infof("Getting block %d of file %s from data server %s", blockMeta.BlockID, blockMeta.FileName, hostName)
					firstBlockData, err = c.getFileBlock(hostName, blockMeta.FileName, blockMeta.BlockID)
					if err != nil {
						logrus.Infof("Failed to get block %d of file %s from data server %s with error %s", blockMeta.BlockID, blockMeta.FileName, hostName, err)
						continue
					}
					logrus.Infof("Got block %d of file %s from data server %s", blockMeta.BlockID, blockMeta.FileName, hostName)
					return nil
				}
				return nil
			})
		}(firstBlock)
		if err := eg.Wait(); err != nil {
			return fmt.Errorf("failed to get the first block: %v", err)
		}
		logrus.Infof("Got the first block of size %d", len(firstBlockData))
	}

	// compute the read offset of the local file
	readSize := map[int64]struct {
		Offset int64
		Size   int64
	}{}
	readSize[blockIDs[0]] = struct {
		Offset int64
		Size   int64
	}{
		Offset: 0,
		Size:   c.blockSize - firstBlock.BlockSize,
	}
	for i := 1; i < len(blockIDs); i++ {
		readSize[blockIDs[i]] = struct {
			Offset int64
			Size   int64
		}{
			Offset: readSize[blockIDs[i-1]].Offset + readSize[blockIDs[i-1]].Size,
			Size:   c.blockSize,
		}
	}

	// write the blocks
	writeSem := semaphore.NewWeighted(10)
	eg, _ := errgroup.WithContext(context.Background())
	for i, blockID := range blockIDs {
		func(sdfsfilename string, index int, blockID int64, blockInfo metadata.BlockInfo) {
			eg.Go(func() error {
				// acquire a semaphore
				err := writeSem.Acquire(context.Background(), 1)
				defer writeSem.Release(1)
				if err != nil {
					return fmt.Errorf("failed to acquire semaphore: %v", err)
				}
				// read a block from localfile
				block := make([]byte, readSize[blockID].Size)
				n, err := localfile.ReadAt(block, readSize[blockID].Offset)
				if err != nil && err != os.ErrNotExist && err != io.EOF {
					return fmt.Errorf("cannot read local file %s: %v", localfilename, err)
				}
				logrus.Infof("Read block %d of file %s with size %d", blockID, localfilename, n)

				// append the first block to the first block
				if index == 0 && len(firstBlockData) != 0 {
					block = append(firstBlockData, block...)
					n += len(firstBlockData)
				}

				for _, hostname := range blockInfo[blockID].HostNames {
					// send the block to the data server
					_, err = c.putFileBlock(hostname, sdfsfilename, blockID, block[:n])
					if err != nil {
						return fmt.Errorf("Failed to put block %d of file %s to data server %s with error %w", blockID, sdfsfilename, hostname, err)
					}
					logrus.Infof("Put block %d of file %s with size %d to data server %s", blockID, sdfsfilename, n, hostname)
				}
				blockMeta := blockInfo[blockID]
				blockMeta.BlockSize = int64(n)
				blockInfo[blockID] = blockMeta
				return nil
			})
		}(sdfsfilename, i, blockID, blockInfo)
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("Failed to put file %s to SDFS: %w", sdfsfilename, err)
	}
	err = c.appendFileOK(leader, sdfsfilename, blockInfo)
	if err != nil {
		return fmt.Errorf("Failed to append file %s OK to leader %s: %w", sdfsfilename, leader, err)
	}
	logrus.Infof("Appended all blocks of file %s to SDFS", sdfsfilename)
	return nil
}

// appendBlockInfo gets the block info for appending to a file from the leader server.
func (c *Client) appendBlockInfo(leader, fileName string, fileSize int64) (metadata.BlockInfo, error) {
	conn, err := grpc.Dial(leader+":"+c.leaderServerPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("cannot connect to %s leaderServer: %v", leader, err)
	}
	defer conn.Close()

	client := leaderServerProto.NewLeaderServerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	r, err := client.AppendBlockInfo(ctx, &leaderServerProto.AppendBlockInfoRequest{
		FileName: fileName,
		FileSize: fileSize,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get block info: %v", err)
	}
	appendBlockInfoReply := r.GetBlockInfo()
	blockInfo := metadata.BlockInfo{}
	for blockID, blockMeta := range appendBlockInfoReply {
		blockInfo[blockID] = metadata.BlockMeta{
			HostNames: blockMeta.HostNames,
			FileName:  blockMeta.FileName,
			BlockID:   blockMeta.BlockID,
			BlockSize: blockMeta.BlockSize,
		}
	}
	return blockInfo, nil
}

func (c *Client) appendFileOK(hostname, fileName string, blockInfo metadata.BlockInfo) error {
	conn, err := grpc.Dial(hostname+":"+c.leaderServerPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("cannot connect to %s leaderServer: %v", hostname, err)
	}
	defer conn.Close()

	client := leaderServerProto.NewLeaderServerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	appendFileOKRequestBlockMeta := map[int64]*leaderServerProto.BlockMeta{}
	for blockID, blockMeta := range blockInfo {
		appendFileOKRequestBlockMeta[blockID] = &leaderServerProto.BlockMeta{
			HostNames: blockMeta.HostNames,
			FileName:  blockMeta.FileName,
			BlockID:   blockMeta.BlockID,
			BlockSize: blockMeta.BlockSize,
		}
	}
	_, err = client.AppendFileOK(ctx, &leaderServerProto.AppendFileOKRequest{
		FileName:  fileName,
		BlockInfo: appendFileOKRequestBlockMeta,
	})
	if err != nil {
		return fmt.Errorf("failed to append file OK: %v", err)
	}
	return nil
}
