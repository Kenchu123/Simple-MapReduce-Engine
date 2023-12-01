package main

import (
	"fmt"
	"math/rand"
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
	mapper.Run(filterMaple)
}

// filterMap
func filterMaple(line string, params []string, keyValues mapper.KeyValues) error {
	// params[0] should be regex pattern
	if len(params) != 1 {
		return fmt.Errorf("Invalid params: %v", params)
	}
	// check if the line matches the regex pattern
	if matched, err := regexp.MatchString(params[0], line); err != nil {
		return fmt.Errorf("Invalid regex pattern: %v", params[0])
	} else if matched {
		// key = random string int 1 to 10
		// value = line
		key := fmt.Sprintf("%d", rand.Intn(10))
		keyValues[key] = append(keyValues[key], line)
	}
	return nil
}

func main() {
	mapleCmd.Execute()
}
