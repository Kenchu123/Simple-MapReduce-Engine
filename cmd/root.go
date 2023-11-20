package cmd

import (
	"github.com/spf13/cobra"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/config"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/delete"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/disable"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/enable"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/fail"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/get"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/join"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/leave"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/list_mem"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/list_self"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/ls"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/metadata"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/multiread"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/multiwrite"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/put"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/serve"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/cmd/store"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/logger"
)

var rootCmd = &cobra.Command{
	Use:   "sdfs",
	Short: "Simple Distributed File System ",
	Long:  `Machine Programming 3 - Simple Distributed File System `,
}
var logPath string

func Execute() error {
	logger.Init(logPath)
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&logPath, "log", "l", "logs/sdfs.log", "path to log file")

	rootCmd.AddCommand(serve.New(), get.New(), put.New(), ls.New(), store.New(), metadata.New(), delete.New(), multiread.New(), multiwrite.New())
	rootCmd.AddCommand(join.New(), leave.New(), fail.New(), config.New(), list_mem.New(), list_self.New(), enable.New(), disable.New())
}
