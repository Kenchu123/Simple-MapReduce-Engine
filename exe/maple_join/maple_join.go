package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/exe/mapper"
)

var mapleCmd = &cobra.Command{
	Use:     "maple <inputfile> <outputprefix> <dataset_name> <column_index>",
	Short:   "maple",
	Long:    "maple runs a map function on the inputfile and outputs to outputprefix",
	Example: "  maple inputfile outputprefix",
	Args:    cobra.MinimumNArgs(2),
	Run:     maple,
}

func maple(cmd *cobra.Command, args []string) {
	mapper := mapper.NewMapper(args[0], args[1], args[2:])
	mapper.Run(joinMaple)
}

// joinMap
func joinMaple(line string, params []string, keyValues mapper.KeyValues) error {
	parts := strings.Split(line, ",")
	// params[0] should be filename
	// params[1] should be index
	if len(params) != 2 {
		return fmt.Errorf("Invalid params: %v", params)
	}
	index, err := strconv.Atoi(params[1])
	if err != nil {
		return fmt.Errorf("Invalid index: %v", params[1])
	}
	if len(parts) > index {
		key := parts[index]
		key = strings.TrimSpace(key)

		if key == "" {
			key = "Empty"
		}
		// replace '/' with '-'
		key = strings.ReplaceAll(key, "/", "-")
		keyValues[key] = append(keyValues[key], fmt.Sprintf("%s,%s", params[0], line))
	}
	return nil
}

func main() {
	mapleCmd.Execute()
}
