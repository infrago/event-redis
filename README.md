# event-redis

`event-redis` 是 `event` 模块的 `redis` 驱动。

## 安装

```bash
go get github.com/infrago/event@latest
go get github.com/infrago/event-redis@latest
```

## 接入

```go
import (
    _ "github.com/infrago/event"
    _ "github.com/infrago/event-redis"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[event]
driver = "redis"
```

## 公开 API（摘自源码）

- `func (d *redisDriver) Connect(inst *event.Instance) (event.Connection, error)`
- `func (c *redisConnection) Open() error`
- `func (c *redisConnection) Close() error`
- `func (c *redisConnection) Register(name, group string) error`
- `func (c *redisConnection) Start() error`
- `func (c *redisConnection) Stop() error`
- `func (c *redisConnection) Publish(name string, data []byte) error`

## 排错

- driver 未生效：确认模块段 `driver` 值与驱动名一致
- 连接失败：检查 endpoint/host/port/鉴权配置
