package scheduler

import (
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	pb "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/scheduler/proto"
)

type Job struct {
	jobID    string
	jobType  string
	params   []string
	stream   pb.Scheduler_PutJobServer
	finished chan<- bool

	taskIDs []string
	tasks   sync.Map
}

// Logf logs a message to the client
func (j *Job) Logf(format string, args ...interface{}) {
	// log to server
	logrus.WithFields(logrus.Fields{
		"jobID": j.jobID,
	}).Infof(format, args...)

	// send message to client
	j.stream.Send(&pb.PutJobResponse{
		JobID:   j.jobID,
		Message: fmt.Sprintf(format, args...),
	})
}

func (j *Job) createMapleTask(taskID, filename, mapleExe, sdfsIntermediateFilenamePrefix string, mapleExeParams []string) {
	task := NewMapleTask(taskID, filename, mapleExe, sdfsIntermediateFilenamePrefix, mapleExeParams)
	j.taskIDs = append(j.taskIDs, taskID)
	j.tasks.Store(taskID, task)
	j.Logf("Task Created: %+v", task)
}

func (j *Job) createJuiceTask(taskID string, filenames []string, juiceExe, sdfsDestFilename, sdfsIntermediateFilenamePrefix string, juiceExeParams []string) {
	task := NewJuiceTask(taskID, filenames, juiceExe, sdfsDestFilename, sdfsIntermediateFilenamePrefix, juiceExeParams)
	j.taskIDs = append(j.taskIDs, taskID)
	j.tasks.Store(taskID, task)
	j.Logf("Task Created: %+v", task)
}
