package main

import (
	"fmt"
	"regexp"

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
	mapper.Run(demoMaple)
}

// wordCountMap
func demoMaple(line string, params []string, keyValues mapper.KeyValues) error {
	// params[0] should be regex pattern
	if len(params) != 1 {
		return fmt.Errorf("Invalid params: %v", params)
	}
	// check if the line matches the regex pattern
	if matched, err := regexp.MatchString(params[0], line); err != nil {
		return fmt.Errorf("Invalid regex pattern: %v", params[0])
	} else if matched {
		keyValues["filter"] = append(keyValues["filter"], fmt.Sprintf("%s", line))
	}
	return nil
}

func main() {
	mapleCmd.Execute()
}
