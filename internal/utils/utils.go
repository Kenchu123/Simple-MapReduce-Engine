package utils

import (
	"os"

	"github.com/xyproto/randomstring"
	client "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/sdfsclient"
)

func GenerateRandomFileName() string {
	randomstring.Seed()
	return randomstring.CookieFriendlyString(10)
}

func CreateLocalFolder(foldername string) error {
	return os.Mkdir(foldername, 0755)
}

func DeleteLocalFolder(foldername string) error {
	return os.RemoveAll(foldername)
}

func DeleteLocalFiles(filenames []string) error {
	for _, filename := range filenames {
		err := os.Remove(filename)
		if err != nil {
			return err
		}
	}
	return nil
}

func ListLocalFiles(foldername string) ([]string, error) {
	var filenames []string
	files, err := os.ReadDir(foldername)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		filenames = append(filenames, file.Name())
	}
	return filenames, nil
}

func ListLocalFilesWithPrefix(foldername, prefix string) ([]string, error) {
	var filenames []string
	files, err := os.ReadDir(foldername)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if file.Name()[0:len(prefix)] == prefix {
			filenames = append(filenames, file.Name())
		}
	}
	return filenames, nil
}

func DeleteSDFSFiles(sdfsClient *client.Client, filenames []string) error {
	for _, filename := range filenames {
		err := sdfsClient.DelFile(filename)
		if err != nil {
			return err
		}
	}
	return nil
}
