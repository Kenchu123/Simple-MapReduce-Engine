package main

import (
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/exe/reducer"
)

var juiceCmd = &cobra.Command{
	Use:     "juice [inputprefix] [outputfile] [params]",
	Short:   "juice",
	Long:    "juice runs a map function on the inputfile and outputs to outputprefix",
	Example: "  juice inputfile outputprefix",
	Args:    cobra.MinimumNArgs(2),
	Run:     juice,
}

func juice(cmd *cobra.Command, args []string) {
	reducer := reducer.NewReducer(args[0], args[1], args[2:])
	reducer.Run(demoReducer)
}

// demo reducer
func demoReducer(lines []string, params []string, keyValues reducer.KeyValue) error {
	if len(lines) < 1 {
		return nil
	}
	partSum := map[string]float32{}
	total := 0
	for _, line := range lines {
		s := strings.Split(line, " ")
		if len(s) < 2 {
			logrus.Fatal("invalid line")
		}
		t := s[1]
		i, err := strconv.Atoi(s[2])
		if err != nil {
			logrus.Fatal(err)
		}
		if _, ok := partSum[t]; !ok {
			partSum[t] = 0
		}
		partSum[t] += float32(i)
		total += i
	}
	for k, v := range partSum {
		partSum[k] = v / float32(total) * 100
		keyValues[k] = strconv.FormatFloat(float64(partSum[k]), 'f', 2, 32) + "%"
	}
	return nil
}

func main() {
	juiceCmd.Execute()
}
