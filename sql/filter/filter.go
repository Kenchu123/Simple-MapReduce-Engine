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
	if len(os.Args) < 8 {
		log.Fatalf("Usage: ./filter <hadoop or sdfs> SELECT ALL FROM <Dataset> WHERE <regex_condition>\n")
	}

	systemType := os.Args[1]
	fullCommand := strings.Join(os.Args[2:], " ")

	if !strings.HasPrefix(fullCommand, "SELECT ALL FROM ") || !strings.Contains(fullCommand, " WHERE ") {
		log.Fatalf("Usage: ./filter <hadoop or sdfs> SELECT ALL FROM <Dataset> WHERE <regex_condition>\n")
	}

	parts := strings.Split(fullCommand, " WHERE ")
	dataset := strings.TrimSpace(strings.TrimPrefix(parts[0], "SELECT ALL FROM"))
	regexCondition := parts[1]

	time_arg := fmt.Sprintf("%d", time.Now().Unix())
	datasetPath := dataset

	if systemType == "hadoop" {
		execHDFSCommand("-put", "-f", datasetPath, "/input")
		fmt.Printf("Upload %s success!\n", dataset)
		jarPath := "./filter.jar"
		inputHadoop := "/input/" + dataset
		outputHadoop := fmt.Sprintf("/output/%s%s", time_arg, dataset)
		execHadoopCommand("jar", jarPath, "Filter", inputHadoop, outputHadoop, regexCondition)
	} else if systemType == "sdfs" {
		maplePath := "./maple_filter"
		juicePath := "./juice_filter"
		intermediate_prefix := fmt.Sprintf("%s_input%s-", time_arg, dataset)
		inputSDFS := fmt.Sprintf("%s_input_%s", time_arg, dataset)
		outputSDFS := fmt.Sprintf("%s_output_%s", time_arg, dataset)
		execSDFSCommand("put", datasetPath, inputSDFS)
		execSDFSCommand("maple", maplePath, "10", intermediate_prefix, inputSDFS, regexCondition)
		execSDFSCommand("juice", juicePath, "10", intermediate_prefix, outputSDFS, "--delete_input", "1")
	} else {
		log.Fatalf("Invalid system type. Use 'hadoop' or 'sdfs'.\n")
	}
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
	cmd := exec.Command("bin/sdfs", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("SDFS Command failed: %v", err)
	}
}
