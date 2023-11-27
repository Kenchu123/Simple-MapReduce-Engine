package metadata

import (
	"fmt"
	"sync"
)

// Metadata handle metadata.
type Metadata struct {
	FileInfo map[string]FileInfo
	mu       sync.RWMutex
}

type FileInfo struct {
	BlockInfo BlockInfo
}

// BlockInfo is the metadata of a file.
type BlockInfo map[int64]BlockMeta // map[blockID]BlockMeta}

// BlockMeta is the metadata of a file block.
type BlockMeta struct {
	HostNames []string
	FileName  string
	BlockID   int64
	BlockSize int64
}

// NewMetadata creates a new metadata.
func NewMetadata() *Metadata {
	return &Metadata{
		FileInfo: make(map[string]FileInfo),
		mu:       sync.RWMutex{},
	}
}

func (m *Metadata) IsFileExist(fileName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.FileInfo[fileName]
	return ok
}

func (m *Metadata) GetFileInfo() map[string]FileInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.FileInfo
}

func (m *Metadata) GetBlockInfo(fileName string) (BlockInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.FileInfo[fileName]; !ok {
		return nil, fmt.Errorf("file %s not found", fileName)
	}
	return m.FileInfo[fileName].BlockInfo, nil
}

// AddOrUpdateFile adds or updates a file to metadata.
func (m *Metadata) AddOrUpdateBlockInfo(fileName string, blockInfo BlockInfo) {
	if !m.IsFileExist(fileName) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.FileInfo[fileName] = FileInfo{
			BlockInfo: blockInfo,
		}
	} else {
		m.mu.Lock()
		defer m.mu.Unlock()
		fileInfo := m.FileInfo[fileName]
		fileInfo.BlockInfo = blockInfo
		m.FileInfo[fileName] = fileInfo
	}
}

func (m *Metadata) GetBlockMeta(fileName string, blockID int64) (BlockMeta, error) {
	blockInfo, err := m.GetBlockInfo(fileName)
	if err != nil {
		return BlockMeta{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := blockInfo[blockID]; !ok {
		return BlockMeta{}, fmt.Errorf("block %d of file %s not found", blockID, fileName)
	}
	return blockInfo[blockID], nil
}

func (m *Metadata) AddOrUpdateBlockMeta(fileName string, blockMeta BlockMeta) error {
	if !m.IsFileExist(fileName) {
		m.mu.Lock()
		m.FileInfo[fileName] = FileInfo{
			BlockInfo: BlockInfo{},
		}
		m.mu.Unlock()
	}
	blockInfo, err := m.GetBlockInfo(fileName)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	blockInfo[blockMeta.BlockID] = blockMeta
	return nil
}

func (m *Metadata) DelFile(fileName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.FileInfo, fileName)
}
