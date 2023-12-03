package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 9 {
		log.Fatalf("Usage: ./join <hadoop or sdfs> SELECT ALL FROM <dataset1>, <dataset2> WHERE <keyFieldName1 = keyFieldName2>\n")
	}

	systemType := os.Args[1]
	fullCommand := strings.Join(os.Args[2:], " ")
	if !strings.HasPrefix(fullCommand, "SELECT ALL FROM ") || !strings.Contains(fullCommand, " WHERE ") {
		log.Fatalf("Usage: ./join <hadoop or sdfs> SELECT ALL FROM <dataset1>, <dataset2> WHERE <keyFieldName1 = keyFieldName2>\n")
	}

	parts := strings.Split(fullCommand, " WHERE ")
	datasetsPart := strings.TrimSpace(strings.TrimPrefix(parts[0], "SELECT ALL FROM"))
	datasets := strings.Split(datasetsPart, ",")
	if len(datasets) != 2 {
		log.Fatalf("You must specify exactly two datasets.\n")
	}

	dataset1 := strings.TrimSpace(datasets[0])
	dataset2 := strings.TrimSpace(datasets[1])

	keyFieldsPart := parts[1]
	keyFields := strings.Split(keyFieldsPart, "=")
	if len(keyFields) != 2 {
		log.Fatalf("You must specify two key fields for joining.\n")
	}

	keyFieldName1 := strings.TrimSpace(keyFields[0])
	keyFieldName2 := strings.TrimSpace(keyFields[1])
	timeArg := fmt.Sprintf("%d", time.Now().Unix())
	keyIndex1, err := getColumnIndex(dataset1, keyFieldName1)
	if err != nil {
		log.Fatalf("Cannot find index according to the key in %s", dataset1)
	}
	keyIndex2, err := getColumnIndex(dataset2, keyFieldName2)
	if err != nil {
		log.Fatalf("Cannot find index according to the key in %s", dataset2)
	}

	if systemType == "hadoop" {
		execHDFSCommand("dfs", "-put", "-f", dataset1, "/input")
		execHDFSCommand("dfs", "-put", "-f", dataset2, "/input")
		fmt.Printf("Upload %s success!\n", dataset1)
		fmt.Printf("Upload %s success!\n", dataset2)
		jarPath := "./join.jar"
		// jarPath2 := "./join2.jar"
		inputHadoop1 := "/input/" + dataset1
		inputHadoop2 := "/input/" + dataset2
		// interHadoop := fmt.Sprintf("/output/inter_%s_%s", timeArg, dataset1)
		outputHadoop := fmt.Sprintf("/output/output_%s_%s", timeArg, dataset1)
		execHadoopCommand("jar", jarPath, "Join", inputHadoop1, inputHadoop2, outputHadoop, strconv.Itoa(keyIndex1), strconv.Itoa(keyIndex2))
		// execHadoopCommand("jar", jarPath2, "Join2", interHadoop, outputHadoop)
	} else if systemType == "sdfs" {
		maplePath := "./maple_join"
		juicePath := "./juice_join"
		intermediate_prefix := fmt.Sprintf("%s_join", timeArg)
		inputSDFS1 := fmt.Sprintf("input_%s_%s", timeArg, dataset1)
		inputSDFS2 := fmt.Sprintf("input_%s_%s", timeArg, dataset2)
		outputSDFS := fmt.Sprintf("output_%s_%s", timeArg, dataset1)
		execSDFSCommand("put", dataset1, inputSDFS1)
		execSDFSCommand("put", dataset2, inputSDFS2)
		execSDFSCommand("maple", maplePath, "10", intermediate_prefix, inputSDFS1, inputSDFS1, strconv.Itoa(keyIndex1))
		execSDFSCommand("maple", maplePath, "10", intermediate_prefix, inputSDFS2, inputSDFS2, strconv.Itoa(keyIndex2))
		execSDFSCommand("juice", juicePath, "10", intermediate_prefix, outputSDFS, "--delete_input", "1")
	} else {
		log.Fatalf("Invalid system type. Use 'hadoop' or 'sdfs'.\n")
	}
	fmt.Printf("Output file name: output_%s_%s\n", timeArg, dataset1)
}

func getColumnIndex(dataset string, keyFieldName string) (int, error) {
	file, err := os.Open(dataset)
	if err != nil {
		return -1, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		headers := strings.Split(scanner.Text(), ",")
		for index, field := range headers {
			if field == keyFieldName {
				return index, nil
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return -1, err
	}

	return -1, fmt.Errorf("column not found")
}

func execHDFSCommand(args ...string) {
	cmd := exec.Command("hdfs", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("HDFS Command failed: %v", err)
	}
}

func execHadoopCommand(args ...string) {
	cmd := exec.Command("hadoop", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("Hadoop Command failed: %v", err)
	}
}
func execSDFSCommand(args ...string) {
	cmd := exec.Command("./sdfs", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("SDFS Command failed: %v", err)
	}
}
