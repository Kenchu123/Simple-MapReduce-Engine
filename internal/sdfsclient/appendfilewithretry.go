package client

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

func (c *Client) AppendFileWithRetry(localfilename, sdfsfilename string) error {
	for i := 0; i < 5; i++ {
		err := c.AppendFile(localfilename, sdfsfilename)
		if err == nil {
			return nil
		} else {
			logrus.Errorf("append file %s to %s failed: %v", localfilename, sdfsfilename, err)
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("append file %s to %s failed", localfilename, sdfsfilename)
}
