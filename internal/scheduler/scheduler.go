package scheduler

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	pb "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/scheduler/proto"
	"google.golang.org/grpc"
)

type Scheduler struct {
	hostname string
	port     string

	pb.UnimplementedSchedulerServer

	jobs sync.Map
}

type Job struct {
	jobID    string
	jobType  string
	params   []string
	stream   pb.Scheduler_PutJobServer
	finished chan<- bool
}

func NewScheduler(hostname, port string) *Scheduler {
	return &Scheduler{
		hostname: hostname,
		port:     port,
	}
}

func (s *Scheduler) Run() {
	// check if self hostname is same as scheduler target
	hostname, err := os.Hostname()
	if err != nil {
		logrus.Errorf("failed to get hostname: %v", err)
		return
	}
	if hostname != s.hostname {
		return
	}

	listen, err := net.Listen("tcp", fmt.Sprintf(":%s", s.port))
	if err != nil {
		logrus.Fatalf("failed to listen on port %s: %v\n", s.port, err)
		return
	}
	defer listen.Close()
	grpcServer := grpc.NewServer()
	pb.RegisterSchedulerServer(grpcServer, s)
	logrus.Infof("Scheduler listening on port %s", s.port)
	if err := grpcServer.Serve(listen); err != nil {
		logrus.Fatalf("failed to serve: %v", err)
		return
	}
}

func (s *Scheduler) PutJob(in *pb.PutJobRequest, stream pb.Scheduler_PutJobServer) error {
	fin := make(chan bool)
	job := Job{
		jobID:    in.GetJobID(),
		jobType:  in.GetType(),
		params:   in.GetParams(),
		stream:   stream,
		finished: fin,
	}
	s.jobs.Store(job.jobID, job)
	ctx := stream.Context()

	// on processing the job
	go func(job Job) {
		// TODO: process the job
		job.stream.Send(&pb.PutJobResponse{
			JobID:   job.jobID,
			Message: "Hello World1",
		})
		logrus.Infof("Sent Message of PutJob to client")
		time.Sleep(2 * time.Second)
		job.stream.Send(&pb.PutJobResponse{
			JobID:   job.jobID,
			Message: "Hello World2",
		})
		logrus.Infof("Sent Message of PutJob to client")
		time.Sleep(2 * time.Second)
		job.stream.Send(&pb.PutJobResponse{
			JobID:   job.jobID,
			Message: "Hello World3",
		})
		logrus.Infof("Sent Message of PutJob to client")
		time.Sleep(2 * time.Second)

		job.finished <- true
	}(job)

	// keep alive to send message to client
	select {
	case <-fin:
		logrus.Infof("closing stream")
		s.jobs.Delete(job.jobID)
		return nil
	case <-ctx.Done():
		logrus.Infof("client disconnect")
		s.jobs.Delete(job.jobID)
		return nil
	}
	return nil
}
