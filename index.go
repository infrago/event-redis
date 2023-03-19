package event_redis

import (
	"github.com/infrago/event"
)

func Driver() event.Driver {
	return &redisDriver{}
}

func init() {
	event.Register("redis", Driver())
}
