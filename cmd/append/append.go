package append

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	client "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/sdfsclient"
)

var configPath string

var appendCmd = &cobra.Command{
	Use:     "append [localfilename] [sdfsfilename]",
	Short:   "append file to SDFS",
	Long:    `append file to SDFS`,
	Example: `  sdfs append local_test sdfs_test`,
	Args:    cobra.ExactArgs(2),
	Run:     append,
}

func append(cmd *cobra.Command, args []string) {
	client, err := client.NewClient(configPath)
	if err != nil {
		logrus.Fatal(err)
	}
	err = client.AppendFile(args[0], args[1])
	if err != nil {
		logrus.Fatal(err)
	}
}

func New() *cobra.Command {
	return appendCmd
}

func init() {
	appendCmd.PersistentFlags().StringVarP(&configPath, "config", "c", ".sdfs/config.yml", "path to config file")
}
