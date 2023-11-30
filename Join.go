package main

import (
    "os"
    "os/exec"
    "log"
    "strings"
    "time"
    "fmt"
)

func main() {
    if len(os.Args) < 8 {
        log.Fatalf("Usage: ./Join SELECT ALL FROM <dataset1>, <dataset2> WHERE <keyFieldName1 = keyFieldName2>\n")
    }

    fullCommand := strings.Join(os.Args[1:], " ")
    if !strings.HasPrefix(fullCommand, "SELECT ALL FROM ") || !strings.Contains(fullCommand, " WHERE ") {
        log.Fatalf("Usage: ./Join SELECT ALL FROM <dataset1>, <dataset2> WHERE <keyFieldName1 = keyFieldName2>\n")
    }

    parts := strings.Split(fullCommand, " WHERE ")
    datasetsPart := strings.TrimSpace(strings.TrimPrefix(parts[0], "SELECT ALL FROM"))
    datasets := strings.Split(datasetsPart, ",")
    if len(datasets) != 2 {
        log.Fatalf("You must specify exactly two datasets.\n")
    }

    keyFieldsPart := parts[1]
    keyFields := strings.Split(keyFieldsPart, "=")
    if len(keyFields) != 2 {
        log.Fatalf("You must specify two key fields for joining.\n")
    }

    keyFieldName1 := strings.TrimSpace(keyFields[0])
    keyFieldName2 := strings.TrimSpace(keyFields[1])

    execHDFSCommand("-put", "-f", datasets[0], "/user/jhihwei2/input")
    execHDFSCommand("-put", "-f", datasets[1], "/user/jhihwei2/input")
    fmt.Println("Datasets uploaded successfully.")

    jarPath := "/home/jhihwei2/hadoop/Joinj.jar"
    inputPath1 := "/user/jhihwei2/input/" + strings.TrimSpace(datasets[0])
    inputPath2 := "/user/jhihwei2/input/" + strings.TrimSpace(datasets[1])
    outputPath := fmt.Sprintf("/user/jhihwei2/output/join_output_%d", time.Now().Unix())

    execHadoopCommand("jar", jarPath, "Join", inputPath1, inputPath2, outputPath, keyFieldName1, keyFieldName2)
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
