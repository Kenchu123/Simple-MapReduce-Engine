package utils

import (
	"os"

	"github.com/xyproto/randomstring"
	client "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/sdfsclient"
)

func GenerateRandomFileName() string {
	randomstring.Seed()
	return "temp-" + randomstring.CookieFriendlyString(10)
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
		if len(file.Name()) >= len(prefix) && file.Name()[0:len(prefix)] == prefix {
			filenames = append(filenames, file.Name())
		}
	}
	return filenames, nil
}

func ListSDFSFilesWithPrefix(sdfsClient *client.Client, prefix string) ([]string, error) {
	var filenames []string
	metadata, err := sdfsClient.GetMetadata()
	if err != nil {
		return nil, err
	}
	for filename := range metadata.GetFileInfo() {
		if len(filename) >= len(prefix) && filename[0:len(prefix)] == prefix {
			filenames = append(filenames, filename)
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
