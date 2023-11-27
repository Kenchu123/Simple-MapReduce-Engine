package client

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
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
	// TODO: Delete this line
	fmt.Println(fileInfo.Size())

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
	return nil
}
