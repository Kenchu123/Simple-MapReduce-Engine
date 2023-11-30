package taskmanager

import (
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/enums"
	client "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/sdfsclient"
	pb "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/taskmanager/proto"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/utils"
)

type Task struct {
	taskID        string
	taskType      string
	exeFilename   string
	inputFilename string
	params        []string
	stream        pb.TaskManager_PutTaskServer
	finished      chan<- bool
	err           chan<- error
}

func (t *Task) Logf(format string, args ...interface{}) {
	// log to server
	logrus.WithFields(logrus.Fields{
		"taskID": t.taskID,
	}).Infof(format, args...)

	// send message to client
	t.stream.Send(&pb.PutTaskResponse{
		TaskID:  t.taskID,
		Message: fmt.Sprintf(format, args...),
	})
}

func (t *TaskManager) PutTask(in *pb.PutTaskRequest, stream pb.TaskManager_PutTaskServer) error {
	fin := make(chan bool)
	err := make(chan error)
	task := &Task{
		taskID:        in.GetTaskID(),
		taskType:      in.GetTaskType(),
		exeFilename:   in.GetExeFilename(),
		inputFilename: in.GetInputFilename(),
		params:        in.GetParams(),
		stream:        stream,
		finished:      fin,
		err:           err,
	}
	ctx := stream.Context()

	go t.processTask(task)

	select {
	case <-ctx.Done():
		logrus.Infof("task %s cancelled", task.taskID)
		return fmt.Errorf("task %s cancelled", task.taskID)
	case err := <-err:
		logrus.Infof("task %s failed: %v", task.taskID, err)
		return fmt.Errorf("task %s failed: %v", task.taskID, err)
	case <-fin:
		logrus.Infof("task %s finished", task.taskID)
		return nil
	}
}

func (t *TaskManager) processTask(task *Task) {
	task.Logf("Task Received %+v", task)
	switch task.taskType {
	case enums.MAPLE:
		err := t.processMapleTask(task)
		if err != nil {
			task.Logf("failed to process maple task: %v", err)
			task.err <- err
			return
		}
	case enums.JUICE:
		err := t.processJuiceTask()
		if err != nil {
			task.Logf("failed to process juice task: %v", err)
			task.err <- err
			return
		}
	}
	task.Logf("Task Finished")
	task.finished <- true
}

func (t *TaskManager) processMapleTask(task *Task) error {
	// Step0: create a folder for intermediate files
	foldername := utils.GenerateRandomFileName()
	err := utils.CreateLocalFolder(foldername)
	if err != nil {
		return err
	}
	logrus.Infof("created folder %s", foldername)
	defer utils.DeleteLocalFolder(foldername)

	// Step1: Download executable from SDFS
	sdfsClient, err := client.NewClient(t.configPath)
	if err != nil {
		return err
	}
	err = sdfsClient.GetFile(task.exeFilename, foldername+"/"+task.exeFilename)
	if err != nil {
		return err
	}
	// Step2: Download input file from SDFS
	err = sdfsClient.GetFile(task.inputFilename, foldername+"/"+task.inputFilename)
	if err != nil {
		return err
	}

	// Step3: Run maple executable
	if err := exec.Command("chmod", "755", foldername+"/"+task.exeFilename).Run(); err != nil {
		return err
	}

	sdfsIntermediateFilenamePrefix := task.params[0]
	args := []string{
		"./" + task.exeFilename,
		task.inputFilename,
		sdfsIntermediateFilenamePrefix,
	}
	args = append(args, task.params[1:]...)
	if err := execCommand(foldername, "bash", "-c", strings.Join(args, " ")); err != nil {
		return err
	}

	// Step4: Upload intermediate files to SDFS
	intermediateFiles, err := utils.ListLocalFilesWithPrefix(foldername, sdfsIntermediateFilenamePrefix)
	if err != nil {
		return err
	}
	// TODO: parallel the AppendFile calls
	for _, filename := range intermediateFiles {
		err := sdfsClient.AppendFile(foldername+"/"+filename, filename)
		if err != nil {
			return err
		}
	}
	task.Logf("Uploaded Intermediate Files to SDFS: %+v", intermediateFiles)
	return nil
}

// execCommand executes a command, retrying if the error is "text file busy"
func execCommand(dir, name string, arg ...string) error {
	exec.Command("sync").Run()
	for i := 0; i < 3; i++ {
		cmd := exec.Command(name, arg...)
		cmd.Dir = dir
		cmd.Stdout = logrus.StandardLogger().Writer()
		cmd.Stderr = logrus.StandardLogger().Writer()
		if err := cmd.Run(); err != nil {
			if exitError, ok := err.(*exec.ExitError); ok {
				if exitError.ExitCode() == 126 {
					// text file busy, retry
					logrus.Infof("text file busy, retrying...")
					time.Sleep(1 * time.Second)
					continue
				}
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("failed to run command %s %s", name, strings.Join(arg, " "))
}

func (t *TaskManager) processJuiceTask() error {
	return nil
}
