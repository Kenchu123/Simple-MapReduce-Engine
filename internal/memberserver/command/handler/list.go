package handler

import (
	"encoding/json"

	"gitlab.engr.illinois.edu/ckchu2/cs425-mp4/internal/memberserver/heartbeat"
)

type ListHandler struct{}

func (h *ListHandler) Handle(args []string) (string, error) {
	instance, err := heartbeat.GetInstance()
	if err != nil {
		return "", err
	}
	b, err := json.MarshalIndent(instance.GetMembership(), "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}
