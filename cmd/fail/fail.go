package fail

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/memberserver/command"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/memberserver/command/client"
)

var configPath string
var machineRegex string

var failCmd = &cobra.Command{
	Use:   "fail",
	Short: "Make the machine fail",
	Long:  `Make the machine fail`,
	Run:   fail,
}

func fail(cmd *cobra.Command, args []string) {
	client, err := client.New(configPath, machineRegex)
	if err != nil {
		logrus.Fatalf("failed to create command client: %v", err)
	}
	results := client.Run([]string{string(command.FAIL)})
	for _, r := range results {
		if r.Err != nil {
			logrus.Errorf("failed to send command to %s: %v\n", r.Hostname, r.Err)
			continue
		}
		logrus.Printf("%s: %s\n", r.Hostname, r.Message)
	}
}

func New() *cobra.Command {
	return failCmd
}

func init() {
	failCmd.PersistentFlags().StringVarP(&configPath, "config", "c", ".sdfs/config.yml", "path to config file")
	failCmd.PersistentFlags().StringVarP(&machineRegex, "machine-regex", "m", ".*", "regex for machines to join (e.g. \"0[1-9]\")")
}
