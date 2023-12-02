package main

import (
	"strings"

	"github.com/spf13/cobra"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/exe/reducer"
)

var juiceCmd = &cobra.Command{
	Use:     "juice <inputprefix> <outputfile>",
	Short:   "juice",
	Long:    "juice runs a map function on the inputfile and outputs to outputprefix",
	Example: "  juice inputfile outputprefix",
	Args:    cobra.MinimumNArgs(2),
	Run:     juice,
}

func juice(cmd *cobra.Command, args []string) {
	reducer := reducer.NewReducer(args[0], args[1], args[2:])
	reducer.Run(joinReducer)
}

// join reducer
func joinReducer(lines []string, params []string, keyValues reducer.KeyValue) error {
	if len(lines) < 1 {
		return nil
	}
	// output format will be (line, "")
	// scan through lines first to find whether it contains two different files
	// use a set to store the filenames
	filenames := make(map[string]bool)
	for _, line := range lines {
		// parts[0] should be filename
		parts := strings.Split(line, ",")
		if len(parts) > 1 {
			key_data := strings.Split(parts[0], " ")
			filenames[key_data[1]] = true
		}
	}
	// if there are two different files, then output the line
	if len(filenames) == 2 {
		for _, line := range lines {
			// delete the first column of every line
			parts := strings.SplitN(line, ",", 2)
			if len(parts) > 1 {
				keyValues[parts[1]] = ""
			}
		}
	}
	return nil
}

func main() {
	juiceCmd.Execute()
}
