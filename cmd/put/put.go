package put

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	client "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/sdfsclient"
)

var configPath string
var retry int

var putCmd = &cobra.Command{
	Use:     "put [localfilename] [sdfsfilename]",
	Short:   "put file from SDFS",
	Long:    `put file from SDFS`,
	Example: `  sdfs put local_test sdfs_test`,
	Args:    cobra.ExactArgs(2),
	Run:     put,
}

func put(cmd *cobra.Command, args []string) {
	client, err := client.NewClient(configPath)
	if err != nil {
		logrus.Fatal(err)
	}
	switch retry {
	case 0:
		err = client.PutFile(args[0], args[1])
	default:
		err = client.PutFileWithRetry(args[0], args[1])
	}
	if err != nil {
		logrus.Fatal(err)
	}
}

func New() *cobra.Command {
	return putCmd
}

func init() {
	putCmd.Flags().IntVarP(&retry, "retry", "r", 1, "retry or not")
	putCmd.PersistentFlags().StringVarP(&configPath, "config", "c", ".sdfs/config.yml", "path to config file")
}
