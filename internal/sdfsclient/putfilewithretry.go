package client

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// PutFileWithRetry puts a file to SDFS with retry
func (c *Client) PutFileWithRetry(localfilename, sdfsfilename string) error {
	for i := 0; i < 5; i++ {
		err := c.PutFile(localfilename, sdfsfilename)
		if err == nil {
			return nil
		}
		logrus.Infof("Put file %s to %s failed, retrying...", localfilename, sdfsfilename)
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("put file %s to %s failed", localfilename, sdfsfilename)
}
