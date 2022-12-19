package redis

import (
	"context"
	"encoding/json"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-redis/redis/v8"
)

func NewStateHandler(client *redis.Client, keyName string) *StateHandler {
	return &StateHandler{client: client, keyName: keyName}
}

type StateHandler struct {
	client  *redis.Client
	keyName string
}

func (r *StateHandler) GetLastPosition() (mysql.Position, error) {
	val, err := r.client.Get(context.Background(), r.keyName).Result()
	if err != nil && err != redis.Nil {
		return mysql.Position{}, err
	}

	if err != nil && err == redis.Nil {
		return mysql.Position{}, nil
	}

	var p mySQLPosition
	err = json.Unmarshal([]byte(val), &p)
	if err != nil {
		return mysql.Position{}, err
	}

	return mysql.Position{
		Name: p.Name,
		Pos:  p.Pos,
	}, nil
}

func (r *StateHandler) SetLastPosition(p mysql.Position) error {
	mp, err := json.Marshal(mySQLPosition{
		Name: p.Name,
		Pos:  p.Pos,
	})
	if err != nil {
		return err
	}

	s := r.client.Set(context.Background(), r.keyName, mp, 0)
	return s.Err()
}

type mySQLPosition struct {
	Name string `json:"name,omitempty"`
	Pos  uint32 `json:"pos,omitempty"`
}
