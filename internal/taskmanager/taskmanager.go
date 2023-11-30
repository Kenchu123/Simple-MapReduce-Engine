package taskmanager

import (
	"fmt"
	"net"

	"github.com/sirupsen/logrus"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/config"
	pb "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/taskmanager/proto"

	"google.golang.org/grpc"
)

type TaskManager struct {
	config     *config.Config
	configPath string
	port       string

	pb.UnimplementedTaskManagerServer
}

func NewTaskManager(config *config.Config, configPath string) *TaskManager {
	return &TaskManager{
		config:     config,
		configPath: configPath,
		port:       config.TaskManager.Port,
	}
}

func (t *TaskManager) Run() {
	listen, err := net.Listen("tcp", fmt.Sprintf(":%s", t.port))
	if err != nil {
		logrus.Fatalf("failed to listen on port %s: %v\n", t.port, err)
		return
	}
	defer listen.Close()
	grpcServer := grpc.NewServer()
	pb.RegisterTaskManagerServer(grpcServer, t)
	logrus.Infof("TaskManager listening on port %s", t.port)
	if err := grpcServer.Serve(listen); err != nil {
		logrus.Fatalf("failed to serve: %v", err)
		return
	}
}
