package juice

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/jobclient"
)

var configPath string
var deleteInput int

var juiceCmd = &cobra.Command{
	Use:     "juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> [params] --delete_input={0,1}",
	Short:   "juice",
	Long:    "juice runs a reduce function on the filename_prefix and outputs to dest_filename",
	Example: "  juice juice_wordcount_regex 5 maple_intermediate_wc_ sdfs_dest/ 'hello.*' --delete_input=1",
	Args:    cobra.MinimumNArgs(4),
	Run:     juice,
}

func juice(cmd *cobra.Command, args []string) {
	client, err := jobclient.NewClient(configPath)
	if err != nil {
		logrus.Fatal(err)
	}
	err = client.Juice(args[0], args[1], args[2], args[3], args[4:], deleteInput)
	if err != nil {
		logrus.Fatal(err)
	}
}

func New() *cobra.Command {
	return juiceCmd
}

func init() {
	juiceCmd.Flags().IntVarP(&deleteInput, "delete_input", "d", 0, "delete input files after juice")
	juiceCmd.PersistentFlags().StringVarP(&configPath, "config", "c", ".sdfs/config.yml", "path to config file")
}
