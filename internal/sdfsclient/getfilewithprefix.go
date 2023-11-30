package client

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

func (c *Client) GetFileWithPrefix(prefix string) ([]string, error) {
	metadata, err := c.GetMetadata()
	if err != nil {
		return nil, err
	}
	mutex := sync.Mutex{}
	fileNames := []string{}
	eg, _ := errgroup.WithContext(context.Background())
	for fileName := range metadata.GetFileInfo() {
		if len(fileName) >= len(prefix) && fileName[:len(prefix)] == prefix {
			func(fileName string) {
				eg.Go(func() error {
					err := c.GetFile(fileName, fileName)
					if err != nil {
						return err
					}
					mutex.Lock()
					defer mutex.Unlock()
					fileNames = append(fileNames, fileName)
					return nil
				})
			}(fileName)
		}
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return fileNames, nil
}
