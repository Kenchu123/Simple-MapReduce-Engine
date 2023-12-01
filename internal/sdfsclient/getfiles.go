package client

import (
	"context"

	"golang.org/x/sync/errgroup"
)

func (c *Client) GetFiles(fileNames []string, prefix string) ([]string, error) {
	eg, _ := errgroup.WithContext(context.Background())
	for _, fileName := range fileNames {
		func(fileName string) {
			eg.Go(func() error {
				err := c.GetFile(fileName, prefix+fileName)
				if err != nil {
					return err
				}
				return nil
			})
		}(fileName)
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return fileNames, nil
}
