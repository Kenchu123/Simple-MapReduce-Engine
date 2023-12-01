package maple

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/jobclient"
)

var configPath string

var mapleCmd = &cobra.Command{
	Use:     "maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory> [params for maple_exe]",
	Short:   "maple",
	Long:    "maple runs a map function on the inputfile and outputs to outputprefix",
	Example: "  maple maple_wordcount_regex 5 maple_intermediate_wc_ sdfs_src/ 'hello.*'",
	Args:    cobra.MinimumNArgs(4),
	Run:     maple,
}

func maple(cmd *cobra.Command, args []string) {
	client, err := jobclient.NewClient(configPath)
	if err != nil {
		logrus.Fatal(err)
	}
	err = client.Maple(args[0], args[1], args[2], args[3], args[4:])
	if err != nil {
		logrus.Fatal(err)
	}
}

func New() *cobra.Command {
	return mapleCmd
}

func init() {
	mapleCmd.PersistentFlags().StringVarP(&configPath, "config", "c", ".sdfs/config.yml", "path to config file")
}
