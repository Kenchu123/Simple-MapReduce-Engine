package taskmanager

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/sirupsen/logrus"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/enums"
	client "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/sdfsclient"
	pb "gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/taskmanager/proto"
	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/utils"
	"golang.org/x/sync/errgroup"
)

type Task struct {
	taskID         string
	taskType       string
	exeFilename    string
	inputFilenames []string
	params         []string
	stream         pb.TaskManager_PutTaskServer
	finished       chan<- bool
	err            chan<- error
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
		taskID:         in.GetTaskID(),
		taskType:       in.GetTaskType(),
		exeFilename:    in.GetExeFilename(),
		inputFilenames: in.GetInputFilenames(),
		params:         in.GetParams(),
		stream:         stream,
		finished:       fin,
		err:            err,
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
	switch task.taskType {
	case enums.MAPLE:
		err := t.processMapleTask(task)
		if err != nil {
			task.Logf("failed to process maple task: %v", err)
			task.err <- err
			return
		}
	case enums.JUICE:
		err := t.processJuiceTask(task)
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
	task.Logf("Processing Maple Task %+v", task)
	// Step0: create a folder for intermediate files
	foldername := utils.GenerateRandomFileName()
	err := utils.CreateLocalFolder(foldername)
	if err != nil {
		return err
	}
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
	// maple should have only one input file
	if len(task.inputFilenames) != 1 {
		return fmt.Errorf("maple should have only one input file")
	}
	inputFilename := task.inputFilenames[0]
	err = sdfsClient.GetFile(inputFilename, foldername+"/"+inputFilename)
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
		inputFilename,
		sdfsIntermediateFilenamePrefix,
	}
	args = append(args, task.params[1:]...)
	if err := execCommand(foldername, "bash", "-c", strings.Join(args, " ")); err != nil {
		return err
	}

	// Step4: Append intermediatefiles to SDFS
	intermediateFiles, err := utils.ListLocalFilesWithPrefix(foldername, sdfsIntermediateFilenamePrefix)
	if err != nil {
		return err
	}
	eg, _ := errgroup.WithContext(context.Background())
	for _, filename := range intermediateFiles {
		func(filename string) {
			eg.Go(func() error {
				err := sdfsClient.AppendFileWithRetry(foldername+"/"+filename, filename)
				if err != nil {
					return err
				}
				return nil
			})
		}(filename)
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	task.Logf("Uploaded Intermediate Files to SDFS: %+v", intermediateFiles)
	return nil
}

// execCommand executes a command, retrying if the error is "text file busy"
func execCommand(dir, name string, arg ...string) error {
	exec.Command("sync").Run()
	for i := 0; i < 1; i++ {
		cmd := exec.Command(name, arg...)
		cmd.Dir = dir
		cmd.Stdout = logrus.StandardLogger().Writer()
		cmd.Stderr = logrus.StandardLogger().Writer()
		if err := cmd.Run(); err != nil {
			if exitError, ok := err.(*exec.ExitError); ok {
				if exitError.ExitCode() == 126 {
					// text file busy, retry
					logrus.Infof("text file busy, retrying...")
					// time.Sleep(1 * time.Second)
					continue
				}
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("failed to run command %s %s", name, strings.Join(arg, " "))
}

func (t *TaskManager) processJuiceTask(task *Task) error {
	task.Logf("Processing Juice Task %+v", task)
	// Step0: create a folder for intermediate files
	foldername := utils.GenerateRandomFileName()
	err := utils.CreateLocalFolder(foldername)
	if err != nil {
		return err
	}
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

	// Step2: Download input files from SDFS
	// juice should have multiple input files
	if len(task.inputFilenames) == 0 {
		return fmt.Errorf("juice should have at least one input file")
	}
	_, err = sdfsClient.GetFiles(task.inputFilenames, foldername+"/")
	if err != nil {
		return err
	}

	// Step3: Run juice executable
	if err := exec.Command("chmod", "755", foldername+"/"+task.exeFilename).Run(); err != nil {
		return err
	}
	sdfsDestFilename := task.params[0]
	sdfsIntermediateFilenamePrefix := task.params[1]
	args := []string{
		"./" + task.exeFilename,
		sdfsIntermediateFilenamePrefix,
		sdfsDestFilename,
	}
	args = append(args, task.params[2:]...)
	if err := execCommand(foldername, "bash", "-c", strings.Join(args, " ")); err != nil {
		return err
	}

	// Step4: Upload output file to SDFS
	err = sdfsClient.AppendFileWithRetry(foldername+"/"+sdfsDestFilename, sdfsDestFilename)
	if err != nil {
		return err
	}
	task.Logf("Uploaded Output File to SDFS: %+v", sdfsDestFilename)
	return nil
}
