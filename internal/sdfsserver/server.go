package sdfsserver

import (
	"sync"

	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/command"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/config"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/dataserver"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/leaderserver"
	memberserver "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/memberserver/command/server"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/scheduler"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/taskmanager"
)

// SDFSServer handles file operations to SDFS.
type SDFSServer struct {
	LeaderServer  *leaderserver.LeaderServer
	DataServer    *dataserver.DataServer
	Memberserver  *memberserver.Server
	CommandServer *command.CommandServer
	Scheduler     *scheduler.Scheduler
	TaskManager   *taskmanager.TaskManager
}

// NewServer creates a new Server.
func NewServer(configPath string) (*SDFSServer, error) {
	config, err := config.NewConfig(configPath)
	if err != nil {
		return nil, err
	}
	leaderServer := leaderserver.NewLeaderServer(config.LeaderServerPort, config.DataServerPort, config.BlockSize, config.RelicationFactor)
	dataServer := dataserver.NewDataServer(config.DataServerPort, config.BlocksDir)
	memberServer := memberserver.NewMemberServer(config.MemberServerPort)
	commandServer := command.NewCommandServer(config.CommandServerPort, configPath)
	scheduler := scheduler.NewScheduler(config, configPath)
	taskManager := taskmanager.NewTaskManager(config, configPath)
	return &SDFSServer{
		LeaderServer:  leaderServer,
		DataServer:    dataServer,
		Memberserver:  memberServer,
		CommandServer: commandServer,
		Scheduler:     scheduler,
		TaskManager:   taskManager,
	}, nil
}

// Run starts the server.
func (s *SDFSServer) Run() {
	var wg sync.WaitGroup
	wg.Add(6)
	go func() {
		defer wg.Done()
		s.LeaderServer.Run()
	}()
	go func() {
		defer wg.Done()
		s.DataServer.Run()
	}()
	go func() {
		defer wg.Done()
		s.Memberserver.Run()
	}()
	go func() {
		defer wg.Done()
		s.CommandServer.Run()
	}()
	go func() {
		defer wg.Done()
		s.Scheduler.Run()
	}()
	go func() {
		defer wg.Done()
		s.TaskManager.Run()
	}()
	wg.Wait()
}
