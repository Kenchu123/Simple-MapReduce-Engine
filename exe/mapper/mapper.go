package mapper

import (
	"bufio"
	"os"

	"github.com/sirupsen/logrus"
)

type KeyValues map[string][]string

type Mapper struct {
	InputFilePath string
	OutputPrefix  string
	Params        []string

	keyValuePairs KeyValues
}

func NewMapper(inputFilePath string, outputPrefix string, params []string) *Mapper {
	return &Mapper{
		InputFilePath: inputFilePath,
		OutputPrefix:  outputPrefix,
		Params:        params,
		keyValuePairs: KeyValues{},
	}
}

func (m *Mapper) Run(mapper func(line string, params []string, keyValues KeyValues) error) {
	// read from input file line by line
	// for each line, run map function
	// append to outputPrefix + "-" + (key)
	inputFile, err := os.Open(m.InputFilePath)
	if err != nil {
		logrus.Fatal(err)
	}
	defer inputFile.Close()
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		if err := mapper(line, m.Params, m.keyValuePairs); err != nil {
			logrus.Fatal(err)
		}
	}
	if err := scanner.Err(); err != nil {
		logrus.Fatal(err)
	}
	// write to the files
	for key, values := range m.keyValuePairs {
		outputPath := m.OutputPrefix + key
		outputFile, err := os.OpenFile(outputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			logrus.Fatal(err)
		}
		defer outputFile.Close()
		for _, value := range values {
			if _, err := outputFile.WriteString(key + " " + value + "\n"); err != nil {
				logrus.Fatal(err)
			}
		}
	}
	// TODO: output filenames
}
