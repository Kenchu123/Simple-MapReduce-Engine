package jobclient

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/config"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/enums"
	schedulerProto "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/scheduler/proto"
	sdfsclient "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/sdfsclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type JobClient struct {
	configPath string
	config     *config.Config
}

func NewClient(configPath string) (*JobClient, error) {
	config, err := config.NewConfig(configPath)
	if err != nil {
		return nil, err
	}
	return &JobClient{
		configPath: configPath,
		config:     config,
	}, nil
}

func (c *JobClient) Maple(mapleExe, numMaples, sdfsIntermediateFileNamePrefix, sdfsSrcDirectory string, mapleExeParams []string) error {
	sdfsClient, err := sdfsclient.NewClient(c.configPath)
	if err != nil {
		return err
	}
	// Put Maple Executable to SDFS
	err = sdfsClient.PutFileWithRetry(mapleExe, mapleExe)
	if err != nil {
		return err
	}
	// Send Job to Scheduler
	params := []string{
		mapleExe,
		numMaples,
		sdfsIntermediateFileNamePrefix,
		sdfsSrcDirectory,
	}
	params = append(params, mapleExeParams...)
	err = c.sendJob(c.config.Scheduler.Hostname, c.config.Scheduler.Port, enums.MAPLE, generateJobID(enums.MAPLE), params)
	if err != nil {
		return err
	}

	return nil
}

func (c *JobClient) Juice(juiceExe, numJuices, sdfsIntermediateFileNamePrefix, sdfsDestFileName string, juiceExeParams []string, deleteInput int, partition string) error {
	sdfsClient, err := sdfsclient.NewClient(c.configPath)
	if err != nil {
		return err
	}

	// Put Juice Executable to SDFS
	err = sdfsClient.PutFileWithRetry(juiceExe, juiceExe)
	if err != nil {
		return err
	}

	// Send Job to Scheduler
	params := []string{
		juiceExe,
		numJuices,
		sdfsIntermediateFileNamePrefix,
		sdfsDestFileName,
		strconv.FormatBool(deleteInput != 0),
		partition,
	}
	params = append(params, juiceExeParams...)
	err = c.sendJob(c.config.Scheduler.Hostname, c.config.Scheduler.Port, enums.JUICE, generateJobID(enums.JUICE), params)
	if err != nil {
		return err
	}

	return nil
}

func (c *JobClient) sendJob(hostname, port, jobType, jobID string, params []string) error {
	conn, err := grpc.Dial(hostname+":"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := schedulerProto.NewSchedulerClient(conn)
	stream, err := client.PutJob(context.Background(), &schedulerProto.PutJobRequest{
		JobID:  jobID,
		Type:   jobType,
		Params: params,
	})
	if err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		jobID := req.GetJobID()
		message := req.GetMessage()
		logrus.WithFields(logrus.Fields{
			"jobID": jobID,
		}).Infof(message)
	}
	return nil
}

func generateJobID(jobType string) string {
	timestamp := time.Now().UnixMicro()
	return fmt.Sprintf("%s-%d", jobType, timestamp)
}
