package event_redis

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bamgoo/bamgoo"
	"github.com/bamgoo/event"
	"github.com/redis/go-redis/v9"
)

func init() {
	bamgoo.Register("redis", &redisDriver{})
}

type (
	redisDriver struct{}

	redisConnection struct {
		mutex    sync.RWMutex
		running  bool
		client   *redis.Client
		instance *event.Instance

		pubsubs map[string]struct{}
		streams map[string]string

		subs []*redis.PubSub
		done chan struct{}
		wg   sync.WaitGroup
	}
)

func (d *redisDriver) Connect(inst *event.Instance) (event.Connection, error) {
	setting := inst.Config.Setting

	addr := "127.0.0.1:6379"
	host := ""
	port := "6379"
	if v, ok := setting["port"].(string); ok && v != "" {
		port = v
	}
	if v, ok := setting["server"].(string); ok && v != "" {
		host = v
	}
	if v, ok := setting["host"].(string); ok && v != "" {
		host = v
	}
	if host != "" {
		addr = host + ":" + port
	}
	if v, ok := setting["addr"].(string); ok && v != "" {
		addr = v
	}

	username, _ := setting["username"].(string)
	password, _ := setting["password"].(string)

	database := 0
	switch v := setting["database"].(type) {
	case int:
		database = v
	case int64:
		database = int(v)
	case float64:
		database = int(v)
	case string:
		if vv, err := strconv.Atoi(v); err == nil {
			database = vv
		}
	}

	return &redisConnection{
		instance: inst,
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Username: username,
			Password: password,
			DB:       database,
		}),
		pubsubs: make(map[string]struct{}, 0),
		streams: make(map[string]string, 0),
		done:    make(chan struct{}),
	}, nil
}

func (c *redisConnection) Open() error {
	return c.client.Ping(context.Background()).Err()
}

func (c *redisConnection) Close() error {
	return c.client.Close()
}

func (c *redisConnection) Register(name, group string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if group == "" {
		c.pubsubs[name] = struct{}{}
		return nil
	}

	stream := streamKey(name)
	c.streams[stream] = group

	err := c.client.XGroupCreateMkStream(context.Background(), stream, group, "$").Err()
	if err != nil && !isBusyGroup(err) {
		return err
	}
	return nil
}

func (c *redisConnection) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.running {
		return nil
	}

	for subject := range c.pubsubs {
		ps := c.client.Subscribe(context.Background(), subject)
		c.subs = append(c.subs, ps)
		ch := ps.Channel()

		c.wg.Add(1)
		go func(eventName string, msgCh <-chan *redis.Message) {
			defer c.wg.Done()
			for {
				select {
				case msg, ok := <-msgCh:
					if !ok {
						return
					}
					c.instance.Submit(func() {
						c.instance.Serve(eventName, []byte(msg.Payload))
					})
				case <-c.done:
					return
				}
			}
		}(subject, ch)
	}

	for stream, group := range c.streams {
		consumer := bamgoo.Generate("event")

		c.wg.Add(1)
		go func(streamName, groupName, consumerName string) {
			defer c.wg.Done()

			for {
				select {
				case <-c.done:
					return
				default:
				}

				res, err := c.client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
					Group:    groupName,
					Consumer: consumerName,
					Streams:  []string{streamName, ">"},
					Count:    16,
					Block:    time.Second,
				}).Result()
				if err != nil {
					if errors.Is(err, redis.Nil) {
						continue
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}

				for _, streamRes := range res {
					for _, msg := range streamRes.Messages {
						raw, _ := msg.Values["data"].(string)
						eventName := subjectFromStream(streamRes.Stream)
						c.instance.Submit(func() {
							c.instance.Serve(eventName, []byte(raw))
						})
						_ = c.client.XAck(context.Background(), streamRes.Stream, groupName, msg.ID).Err()
					}
				}
			}
		}(stream, group, consumer)
	}

	c.running = true
	return nil
}

func (c *redisConnection) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.running {
		return nil
	}

	close(c.done)
	for _, sub := range c.subs {
		_ = sub.Close()
	}
	c.wg.Wait()
	c.subs = nil
	c.done = make(chan struct{})
	c.running = false
	return nil
}

func (c *redisConnection) Publish(name string, data []byte) error {
	if isPublishSubject(name) {
		_, err := c.client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: streamKey(name),
			Values: map[string]any{
				"data": string(data),
			},
		}).Result()
		return err
	}
	return c.client.Publish(context.Background(), name, data).Err()
}

func isBusyGroup(err error) bool {
	if err == nil {
		return false
	}
	return len(err.Error()) >= 9 && err.Error()[:9] == "BUSYGROUP"
}

func isPublishSubject(name string) bool {
	return strings.Contains(name, ".publish.") || strings.HasPrefix(name, "publish.")
}

func streamKey(subject string) string {
	return "event:stream:" + subject
}

func subjectFromStream(stream string) string {
	return strings.TrimPrefix(stream, "event:stream:")
}

var _ event.Connection = (*redisConnection)(nil)
