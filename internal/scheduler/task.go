package scheduler

import (
	"hash/fnv"

	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/enums"
)

type Task struct {
	taskID        string
	taskType      string
	exeFilename   string
	inputFilename string
	params        []string
}

func NewMapleTask(id string, filename string, mapleExe string, sdfsIntermediateFileNamePrefix string, mapleExeParams []string) *Task {
	params := []string{
		sdfsIntermediateFileNamePrefix,
	}
	params = append(params, mapleExeParams...)
	return &Task{
		taskID:        id,
		taskType:      enums.MAPLE,
		exeFilename:   mapleExe,
		inputFilename: filename,
		params:        params,
	}
}

func (t *Task) Hash() uint64 {
	h := fnv.New64a()
	h.Write([]byte(t.taskID))
	return h.Sum64()
}
