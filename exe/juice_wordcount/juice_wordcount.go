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
	reducer.Run(wordcountReducer)
}

// wordcount reducer
func wordcountReducer(lines []string, params []string, keyValues reducer.KeyValue) error {
	total := 0
	// extract key
	if len(lines) < 1 {
		return nil
	}
	// count the total values
	key := strings.Split(lines[0], " ")[0]
	for _, line := range lines {
		s := strings.Split(line, " ")
		if len(s) < 2 {
			logrus.Fatal("invalid line")
		}
		value := strings.Join(s[1:], " ")
		i, err := strconv.Atoi(value)
		if err != nil {
			logrus.Fatal(err)
		}
		total += i
	}
	keyValues[key] = strconv.Itoa(total)
	return nil
}

func main() {
	juiceCmd.Execute()
}
