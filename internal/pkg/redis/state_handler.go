package redis

import (
	"context"
	"strconv"

	"github.com/go-redis/redis/v8"
)

func NewStateHandler(client *redis.Client, keyName string) *StateHandler {
	return &StateHandler{client: client, keyName: keyName}
}

type StateHandler struct {
	client  *redis.Client
	keyName string
}

func (r *StateHandler) GetLastPositionRead() (uint32, error) {
	_, err := r.client.SetNX(context.Background(), r.keyName, 0, 0).Result()
	if err != nil {
		return 0, err
	}

	val, err := r.client.Get(context.Background(), r.keyName).Result()
	if err != nil {
		return 0, err
	}

	lastPos, err := strconv.ParseUint(val, 10, 32)
	if err != nil {
		return 0, err
	}

	return uint32(lastPos), err
}

func (r *StateHandler) SetLastPositionRead(u uint32) error {
	s := r.client.Set(context.Background(), r.keyName, u, 0)
	return s.Err()
}
