package main

import (
	"strings"
	
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
	reducer.Run(filterReducer)
}

// filter reducer
func filterReducer(lines []string, params []string, keyValues reducer.KeyValue) error {
	if len(lines) < 1 {
		return nil
	}
	// output format will be (key, _)
	for _, line := range lines {
		index := strings.Index(line, "")
		if index == -1 {
			continue;
		}
		line = line[index+1:]
		keyValues[line] = ""
	}
	return nil
}

func main() {
	juiceCmd.Execute()
}
