package join

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/memberserver/command"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/memberserver/command/client"
)

var configPath string
var machineRegex string

var joinCmd = &cobra.Command{
	Use:   "join",
	Short: "Show the id of the current node",
	Long:  `Show the id of the current node`,
	Run:   join,
}

func join(cmd *cobra.Command, args []string) {
	client, err := client.New(configPath, machineRegex)
	if err != nil {
		logrus.Fatalf("failed to create command client: %v", err)
	}
	results := client.Run([]string{string(command.JOIN)})
	for _, r := range results {
		if r.Err != nil {
			logrus.Errorf("failed to send command to %s: %v\n", r.Hostname, r.Err)
			continue
		}
		logrus.Printf("%s: %s\n", r.Hostname, r.Message)
	}
}

func New() *cobra.Command {
	return joinCmd
}

func init() {
	joinCmd.PersistentFlags().StringVarP(&configPath, "config", "c", ".sdfs/config.yml", "path to config file")
	joinCmd.PersistentFlags().StringVarP(&machineRegex, "machine-regex", "m", ".*", "regex for machines to join (e.g. \"0[1-9]\")")
}
