package scheduler

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/sirupsen/logrus"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/config"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/enums"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/memberserver/heartbeat"
	pb "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/scheduler/proto"
	client "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/sdfsclient"
	taskManagerProto "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/taskmanager/proto"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Scheduler struct {
	config     *config.Config
	configPath string
	hostname   string
	port       string

	pb.UnimplementedSchedulerServer

	jobs    sync.Map
	jobLock sync.Mutex
}

func NewScheduler(config *config.Config, configPath string) *Scheduler {
	return &Scheduler{
		config:     config,
		configPath: configPath,
		hostname:   config.Scheduler.Hostname,
		port:       config.Scheduler.Port,
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
	job := &Job{
		jobID:    in.GetJobID(),
		jobType:  in.GetType(),
		params:   in.GetParams(),
		stream:   stream,
		finished: fin,
	}
	s.jobs.Store(job.jobID, job)
	ctx := stream.Context()

	// on processing the job
	go s.processJob(job)

	// keep alive to send message to client
	select {
	case <-fin:
		s.jobs.Delete(job.jobID)
		return nil
	case <-ctx.Done():
		logrus.Infof("Client of Job %s Disconnect", job.jobID)
		s.jobs.Delete(job.jobID)
		return nil
	}
}

func (s *Scheduler) processJob(job *Job) {
	job.Logf("Job Received, %+v", job)
	// wait for the job to be processed
	job.Logf("Waiting to be processed")
	s.jobLock.Lock()
	defer s.jobLock.Unlock()

	// process the job
	switch job.jobType {
	case enums.MAPLE:
		err := s.processMapleJob(job)
		if err != nil {
			job.Logf("Error Processing Job: %v", err)
		}
	case enums.JUICE:
		s.processJuiceJob(job)
	}
	// send job finished message to client
	job.Logf("Finished")
	job.finished <- true
}

func (s *Scheduler) processMapleJob(job *Job) error {
	job.Logf("Start Processing")

	mapleExe := job.params[0]
	numMaples, _ := strconv.Atoi(job.params[1])
	sdfsIntermediateFilenamePrefix := job.params[2]
	sdfsSrcDirectory := job.params[3]
	mapleExeParams := job.params[4:]

	// get the files that prefix with 'sdfs_src_directory-' from sdfs
	sdfsClient, err := client.NewClient(s.configPath)
	if err != nil {
		return err
	}
	filenames, err := sdfsClient.GetFileWithPrefix(sdfsSrcDirectory)
	if err != nil {
		return err
	}
	defer utils.DeleteLocalFiles(filenames)
	job.Logf("Downloaded Files from SDFS to the Scheduler: %+v", filenames)
	job.Logf("Spliting the Job into %d Tasks", numMaples)

	// get the total number of lines in the files
	var totalLines int64 = 0
	for _, filename := range filenames {
		err := func() error {
			file, err := os.Open(filename)
			if err != nil {
				return err
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			scanner.Split(bufio.ScanLines)

			for scanner.Scan() {
				totalLines++
			}
			if err := scanner.Err(); err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}

	// split the job into multiple tasks
	var taskCnt int64 = 0
	for _, filename := range filenames {
		err := func() error {
			file, err := os.Open(filename)
			if err != nil {
				return err
			}
			defer file.Close()

			// read the file to task_input_lines and split it into multiple tasks
			taskInputLines := totalLines / int64(numMaples)
			taskInput := []string{}
			var lineCnt int64 = 0
			reader := bufio.NewReader(file)
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					break
				}
				taskInput = append(taskInput, line)
				lineCnt++
				if lineCnt == taskInputLines {
					// put the part of the file into sdfs
					taskID := fmt.Sprintf("%s-%d", job.jobID, taskCnt)
					sdfsClient.PutLines(taskInput, taskID)
					// construct a new task and add into the task queue
					job.createMapleTask(taskID, taskID, mapleExe, sdfsIntermediateFilenamePrefix, mapleExeParams)
					taskCnt++
					taskInput = []string{}
					lineCnt = 0
				}
			}
			if len(taskInput) != 0 {
				// put the part of the file into sdfs
				taskID := fmt.Sprintf("%s-%d", job.jobID, taskCnt)
				sdfsClient.PutLines(taskInput, taskID)
				// construct a new task and add into the task queue
				job.createMapleTask(taskID, taskID, mapleExe, sdfsIntermediateFilenamePrefix, mapleExeParams)
				taskCnt++
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	defer utils.DeleteSDFSFiles(sdfsClient, job.taskIDs)

	// scheduler schedules jobs' tasks to workers
	err = s.scheduleTasks(job)
	if err != nil {
		return err
	}

	return nil
}

func (s *Scheduler) processJuiceJob(job *Job) {
	// split the job into multiple tasks
	// send tasks to workers
	job.Logf("Start Processing")
}

func (s *Scheduler) scheduleTasks(job *Job) error {
	job.Logf("Scheduling Tasks to Workers")
	// schedule tasks to workers
	// TODO: schedule on hash or range
	wg := sync.WaitGroup{}
	for _, taskID := range job.taskIDs {
		task, ok := job.tasks.Load(taskID)
		if !ok {
			return fmt.Errorf("task %s not found", taskID)
		}
		// TODO: parallel
		wg.Add(1)
		go func(job *Job, task *Task) {
			defer wg.Done()
			err := s.scheduleTask(job, task)
			if err != nil {
				job.Logf("Error Scheduling Task %s: %v", task.taskID, err)
			}
		}(job, task.(*Task))
	}
	wg.Wait()
	return nil
}

func (s *Scheduler) getWorkers() ([]string, error) {
	heartbeat, err := heartbeat.GetInstance()
	if err != nil {
		return nil, err
	}
	_membership := heartbeat.GetMembership()
	if _membership == nil {
		return nil, fmt.Errorf("membership is nil")
	}
	members := _membership.GetAliveMembers()
	workers := []string{}
	for _, member := range members {
		workers = append(workers, member.GetName())
	}
	return workers, nil
}

// scheduleTask will re-schedule on failure
func (s *Scheduler) scheduleTask(job *Job, task *Task) error {
	workers, err := s.getWorkers()
	if err != nil {
		return err
	}
	worker := workers[task.Hash()%uint64(len(workers))]
	err = s.putTask(job, task, worker)
	if err != nil {
		// reschedule to retry the task
		return s.scheduleTask(job, task)
	}
	return nil
}

func (s *Scheduler) putTask(job *Job, task *Task, worker string) error {
	job.Logf("Sending Task %s to Worker %s", task.taskID, worker)
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", worker, s.config.TaskManager.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := taskManagerProto.NewTaskManagerClient(conn)
	stream, err := client.PutTask(context.Background(), &taskManagerProto.PutTaskRequest{
		TaskID:        task.taskID,
		TaskType:      task.taskType,
		ExeFilename:   task.exeFilename,
		InputFilename: task.inputFilename,
		Params:        task.params,
	})
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		job.Logf("Task %s: %s", resp.GetTaskID(), resp.GetMessage())
	}
	return nil
}
