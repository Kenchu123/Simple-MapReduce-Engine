package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 7 {
		log.Fatalf("Usage: ./Filter <hadoop or sdfs> SELECT ALL FROM <Dataset> WHERE <regex_condition>\n")
	}

	// Concatenate arguments to form the full command
	fullCommand := strings.Join(os.Args[1:], " ")

	// Check if the command is in the correct format
	if !strings.HasPrefix(fullCommand, "SELECT ALL FROM ") || !strings.Contains(fullCommand, " WHERE ") {
		log.Fatalf("Usage: ./Filter SELECT ALL FROM <Dataset> WHERE <regex_condition>\n")
	}

	// Extract dataset and regex condition
	parts := strings.Split(fullCommand, " WHERE ")
	dataset := strings.TrimSpace(strings.TrimPrefix(parts[0], "SELECT ALL FROM"))
	regexCondition := parts[1]

	// Step 1: Clear input and output directories on HDFS
	//execHDFSCommand("-rm", "-r", "/user/jhihwei2/input/*")
	//execHDFSCommand("-rm", "-r", "/user/jhihwei2/output/*")

	// Step 2: Upload new dataset
	datasetPath := dataset
	execHDFSCommand("-put", "-f", datasetPath, "/user/jhihwei2/input")
	fmt.Printf("Upload %s success!\n", dataset)

	// Step 3: Execute MapReduce job
	jarPath := "/home/jhihwei2/hadoop/Filterj.jar"
	inputPath := "/user/jhihwei2/input/" + dataset
	outputPath := fmt.Sprintf("/user/jhihwei2/output/%s_%d", dataset, time.Now().Unix())
	execHadoopCommand("jar", jarPath, "Filter", inputPath, outputPath, regexCondition)
}

func execHDFSCommand(args ...string) {
	hdfsArgs := append([]string{"dfs"}, args...)
	cmd := exec.Command("/home/jhihwei2/hadoop/bin/hdfs", hdfsArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("HDFS Command failed: %v", err)
	}
}

func execHadoopCommand(args ...string) {
	cmd := exec.Command("/home/jhihwei2/hadoop/bin/hadoop", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("Hadoop Command failed: %v", err)
	}
}
