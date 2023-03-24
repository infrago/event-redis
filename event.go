package event_redis

import (
	"errors"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/infrago/event"
	"github.com/infrago/log"
	"github.com/infrago/util"
)

var (
	errInvalidConnection = errors.New("Invalid event connection.")
	errAlreadyRunning    = errors.New("Redis event is already running.")
	errNotRunning        = errors.New("Redis event is already running.")
)

type (
	redisDriver  struct{}
	redisConnect struct {
		mutex  sync.RWMutex
		client *redis.Pool

		running bool
		actives int64

		instance *event.Instance
		setting  redisSetting

		events map[string]string
		subs   []redis.Conn
	}
	//配置文件
	redisSetting struct {
		Server   string //服务器地址，ip:端口
		Password string //服务器auth密码
		Database string //数据库

		Idle    int //最大空闲连接
		Active  int //最大激活连接，同时最大并发
		Timeout time.Duration
	}

	defaultMsg struct {
		name string
		data []byte
	}
)

// 连接
func (driver *redisDriver) Connect(inst *event.Instance) (event.Connect, error) {
	//获取配置信息
	setting := redisSetting{
		Server: "127.0.0.1:6379", Password: "", Database: "",
		Idle: 30, Active: 100, Timeout: 240,
	}

	if vv, ok := inst.Config.Setting["server"].(string); ok && vv != "" {
		setting.Server = vv
	}
	if vv, ok := inst.Config.Setting["password"].(string); ok && vv != "" {
		setting.Password = vv
	}

	//数据库，redis的0-16号
	if v, ok := inst.Config.Setting["database"].(string); ok {
		setting.Database = v
	}

	if vv, ok := inst.Config.Setting["idle"].(int64); ok && vv > 0 {
		setting.Idle = int(vv)
	}
	if vv, ok := inst.Config.Setting["active"].(int64); ok && vv > 0 {
		setting.Active = int(vv)
	}
	if vv, ok := inst.Config.Setting["timeout"].(int64); ok && vv > 0 {
		setting.Timeout = time.Second * time.Duration(vv)
	}
	if vv, ok := inst.Config.Setting["timeout"].(string); ok && vv != "" {
		td, err := util.ParseDuration(vv)
		if err == nil {
			setting.Timeout = td
		}
	}

	return &redisConnect{
		instance: inst, setting: setting,
		events: make(map[string]string, 0),
		subs:   make([]redis.Conn, 0),
	}, nil
}

// 打开连接
func (this *redisConnect) Open() error {
	this.client = &redis.Pool{
		MaxIdle: this.setting.Idle, MaxActive: this.setting.Active, IdleTimeout: this.setting.Timeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", this.setting.Server)
			if err != nil {
				log.Warning("event.redis.dial", err)
				return nil, err
			}

			//如果有验证
			if this.setting.Password != "" {
				if _, err := c.Do("AUTH", this.setting.Password); err != nil {
					c.Close()
					log.Warning("event.redis.auth", err)
					return nil, err
				}
			}
			//如果指定库
			if this.setting.Database != "" {
				if _, err := c.Do("SELECT", this.setting.Database); err != nil {
					c.Close()
					log.Warning("event.redis.select", err)
					return nil, err
				}
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	//打开一个试一下
	conn := this.client.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	return nil
}

func (this *redisConnect) Health() (event.Health, error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return event.Health{Workload: this.actives}, nil
}

// 关闭连接
func (this *redisConnect) Close() error {
	if this.client != nil {
		if err := this.client.Close(); err != nil {
			return err
		}
		this.client = nil
	}
	return nil
}

func (this *redisConnect) Register(name, group string) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.events[name] = group

	return nil
}

// 开始订阅者
func (this *redisConnect) Start() error {
	if this.running {
		return errAlreadyRunning
	}

	//这个循环，用来从redis读消息
	for key, _ := range this.events {
		subKey := key
		// subGroup := val
		//redis暂不支持分组特性
		//可能需要分布锁来加持，暂时不处理

		conn, err := this.client.Dial()
		if err != nil {
			return err
		}

		//记录
		this.subs = append(this.subs, conn)

		psc := redis.PubSubConn{Conn: conn}
		err = psc.Subscribe(subKey)
		if err != nil {
			return err
		}

		//走全局的池
		this.instance.Submit(func() {
			for {
				rec := psc.Receive()
				if msg, ok := rec.(redis.Message); ok {
					// 走协程执行，不阻塞，因为是事件，和队列不同
					this.instance.Submit(func() {
						this.instance.Serve(subKey, msg.Data)
					})
				} else if _, ok := rec.(error); ok {
					break //退出循环
				}
			}
		})
	}

	this.running = true
	return nil
}

// 停止订阅
func (this *redisConnect) Stop() error {
	if false == this.running {
		return errNotRunning
	}

	//关闭订阅
	for _, sub := range this.subs {
		sub.Close()
	}

	this.running = false
	return nil
}

func (this *redisConnect) Publish(name string, data []byte) error {
	if this.client == nil {
		return errInvalidConnection
	}

	conn := this.client.Get()
	defer conn.Close()

	//写入
	_, err := conn.Do("PUBLISH", name, data)

	if err != nil {
		log.Warning("event.redis.publish", err)
		return err
	}

	return nil
}
