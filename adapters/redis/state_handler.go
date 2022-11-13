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
	val, err := r.client.Get(context.Background(), r.keyName).Result()
	if err != nil && err != redis.Nil {
		return 0, err
	}

	if err != nil && err == redis.Nil {
		val = "0"
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
