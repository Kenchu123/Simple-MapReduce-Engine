package reducer

import (
	"bufio"
	"os"

	"github.com/sirupsen/logrus"
)

type KeyValue map[string]string

type Reducer struct {
	InputFilePrefix string
	OutputFilePath  string
	Params          []string

	keyValuePairs KeyValue
}

func NewReducer(inputFilePrefix, outputFilePath string, params []string) *Reducer {
	return &Reducer{
		InputFilePrefix: inputFilePrefix,
		OutputFilePath:  outputFilePath,
		Params:          params,
		keyValuePairs:   KeyValue{},
	}
}

func (r *Reducer) Run(reducer func(lines []string, params []string, keyValues KeyValue) error) {
	// find the filenames with the same prefix
	inputFilePaths := []string{}
	files, err := os.ReadDir(".")
	if err != nil {
		logrus.Fatal(err)
	}
	for _, file := range files {
		if len(file.Name()) >= len(r.InputFilePrefix) && file.Name()[0:len(r.InputFilePrefix)] == r.InputFilePrefix {
			inputFilePaths = append(inputFilePaths, file.Name())
		}
	}

	// for each file, run reduce function
	for _, inputFilePath := range inputFilePaths {
		inputFile, err := os.Open(inputFilePath)
		if err != nil {
			logrus.Fatal(err)
		}
		defer inputFile.Close()
		lines := []string{}
		scanner := bufio.NewScanner(inputFile)
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			logrus.Fatal(err)
		}
		if err := reducer(lines, r.Params, r.keyValuePairs); err != nil {
			logrus.Fatal(err)
		}
	}
	// write to a file
	outputFile, err := os.Create(r.OutputFilePath)
	if err != nil {
		logrus.Fatal(err)
	}
	defer outputFile.Close()
	for key, values := range r.keyValuePairs {
		if _, err := outputFile.WriteString(key + " " + values + "\n"); err != nil {
			logrus.Fatal(err)
		}
	}
}
