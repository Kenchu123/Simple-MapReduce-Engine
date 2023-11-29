package juice

import (
	"github.com/spf13/cobra"
)

var configPath string
var delete_input int

var juiceCmd = &cobra.Command{
	Use:     "juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> --delete_input={0,1}",
	Short:   "juice",
	Long:    "juice runs a map function on the inputfile and outputs to outputprefix",
	Example: "  juice wordcount 5 juice_intermediate_wc_ sdfs_src/",
	Args:    cobra.ExactArgs(4),
	Run:     juice,
}

func juice(cmd *cobra.Command, args []string) {
}

func New() *cobra.Command {
	return juiceCmd
}

func init() {
	juiceCmd.Flags().IntVarP(&delete_input, "delete_input", "d", 0, "delete input files after juice")
	juiceCmd.PersistentFlags().StringVarP(&configPath, "config", "c", ".sdfs/config.yml", "path to config file")
}
