package event_redis

import (
	"github.com/infrago/event"
	"github.com/infrago/infra"
)

func Driver() event.Driver {
	return &redisDriver{}
}

func init() {
	infra.Register("redis", Driver())
}
