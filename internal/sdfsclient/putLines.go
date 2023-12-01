package client

import (
	"fmt"
	"os"
)

func (c *Client) PutLines(lines []string, fileName string) error {
	// put the lines into a temp file
	tempFileName := fmt.Sprintf("%s.temp", fileName)
	file, err := os.Create(tempFileName)
	if err != nil {
		return err
	}
	defer os.Remove(tempFileName)
	for _, line := range lines {
		_, err = file.WriteString(line + "\n")
		if err != nil {
			return err
		}
	}
	file.Close()

	// put the temp file into sdfs
	return c.PutFileWithRetry(tempFileName, fileName)
}
