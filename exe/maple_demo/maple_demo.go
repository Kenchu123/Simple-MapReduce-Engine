package main

import (
	"fmt"
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
	mapper.Run(demoMaple)
}

// demoMap
func demoMaple(line string, params []string, keyValues mapper.KeyValues) error {
	parts := strings.Split(line, ",")
	// params[0] shsould be {Radio, Fiber, Fiber/Radio, None}
	if len(params) != 1 {
		return fmt.Errorf("Invalid params: %v", params)
	}
	if len(parts) > 10 && parts[10] == params[0] {
		key := parts[9]
		key = strings.TrimSpace(key)

		if key == "" {
			key = "Empty"
		}
		// replace '/' with '-'
		key = strings.ReplaceAll(key, "/", "-")
		keyValues["demo"] = append(keyValues["demo"], fmt.Sprintf("%s 1", key))
	}
	return nil
}

func main() {
	mapleCmd.Execute()
}
