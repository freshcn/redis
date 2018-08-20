# Redis client for Golang [![Build Status](https://travis-ci.org/go-redis/redis.png?branch=v5)](https://travis-ci.org/go-redis/redis)

Supports:

- Redis 3 commands except QUIT, MONITOR, SLOWLOG and SYNC.

API docs: https://godoc.org/gopkg.in/freshcn/redis.v5.

Examples: https://godoc.org/gopkg.in/freshcn/redis.v5#pkg-examples.

## Installation

Install:

```shell
go get -u -v gitee.com/JMArch/redis.v5
mv $GOPARH/src/gitee.com/JMArch/redis.v5 $GOPARH/src/gopkg.in/freshcn/redis.v5
```

Import:

```go
import "gopkg.in/freshcn/redis.v5"
```

## Quickstart

```go
func ExampleNewRingClient() {
	client := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"s1": ":6379",
			"s2": ":6380",
			"s3": ":6381",
			"s4": ":6382",
		},
		Weights: map[string]int{ // 配置节点权重，取值范围 (0, 100]。未配置节点默认为100
			"s1": 100,
			"s2": 50,
			"s3": 25,
			// "s4": 100,
		},
		PollMode:           true, // 启用轮询模式, 权重会影响每个节点的轮询次数
		MoveShards:         true, // 开启节点剔除
		DB:                 0,
		Password:           "",
		HeartbeatFrequency: time.Millisecond * 10,
	})

	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
	// Output: PONG <nil>
}

func ExampleClient() {
	err := client.Set("key", "value", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := client.Get("key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	val2, err := client.Get("key2").Result()
	if err == redis.Nil {
		fmt.Println("key2 does not exists")
	} else if err != nil {
		panic(err)
	} else {
		fmt.Println("key2", val2)
	}
	// Output: key value
	// key2 does not exists
}
```

