package main

import (
	"strings"

	"github.com/spf13/cobra"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/exe/mapper"
)

var mapleCmd = &cobra.Command{
	Use:     "maple [inputfile] [outputprefix] [params]",
	Short:   "maple",
	Long:    "maple runs a map function on the inputfile and outputs to outputprefix",
	Example: "  maple inputfile outputprefix",
	Args:    cobra.MinimumNArgs(2),
	Run:     maple,
}

func maple(cmd *cobra.Command, args []string) {
	mapper := mapper.NewMapper(args[0], args[1], args[2:])
	mapper.Run(wordCountMap)
}

// wordCountMap
func wordCountMap(line string, params []string, keyValues mapper.KeyValues) error {
	for _, word := range strings.Split(line, " ") {
		keyValues[word] = append(keyValues[word], "1")
	}
	return nil
}

func main() {
	mapleCmd.Execute()
}
