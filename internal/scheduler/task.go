package scheduler

import (
	"hash/fnv"

	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/enums"
)

type Task struct {
	taskID         string
	taskType       string
	exeFilename    string
	inputFilenames []string
	params         []string
}

func NewMapleTask(id string, filename string, mapleExe string, sdfsIntermediateFileNamePrefix string, mapleExeParams []string) *Task {
	params := []string{
		sdfsIntermediateFileNamePrefix,
	}
	params = append(params, mapleExeParams...)
	return &Task{
		taskID:         id,
		taskType:       enums.MAPLE,
		exeFilename:    mapleExe,
		inputFilenames: []string{filename},
		params:         params,
	}
}

func NewJuiceTask(id string, filenames []string, juiceExe string, sdfsDestFilename string, sdfsIntermediateFileNamePrefix string, juiceExeParams []string) *Task {
	params := []string{
		sdfsDestFilename,
		sdfsIntermediateFileNamePrefix,
	}
	params = append(params, juiceExeParams...)
	return &Task{
		taskID:         id,
		taskType:       enums.JUICE,
		exeFilename:    juiceExe,
		inputFilenames: filenames,
		params:         params,
	}
}

func (t *Task) Hash() uint64 {
	h := fnv.New64a()
	h.Write([]byte(t.taskID))
	return h.Sum64()
}
