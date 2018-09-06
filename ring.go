package redis

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/freshcn/redis.v5/internal"
	"gopkg.in/freshcn/redis.v5/internal/consistenthash"
	"gopkg.in/freshcn/redis.v5/internal/hashtag"
	"gopkg.in/freshcn/redis.v5/internal/pool"
)

const threshold = 3
const nreplicas = 100

var errRingShardsDown = errors.New("redis: all ring shards are down")

// RingOptions are used to configure a ring client and should be
// passed to NewRing.
type RingOptions struct {
	// Map of name => host:port addresses of ring shards.
	Addrs map[string]string

	// Map of name => Weight of ring shards.
	// 如果某addr的weights不存在，则默认为nreplicas
	// Weight取值范围(0,nreplicas]
	Weights map[string]int

	// if true: 轮流使用ring shards，而不是通过consistenthash方式
	// if false: 使用consistenthash方式
	// 轮询模式也会根据权重来轮询
	PollMode bool

	namesLocker sync.RWMutex
	names       []string
	index       uint32

	// if true: 会自动剔除坏节点
	// if false: 不会自动剔除坏节点
	MoveShards bool

	// Frequency of PING commands sent to check shards availability.
	// Shard is considered down after 3 subsequent failed checks.
	HeartbeatFrequency time.Duration

	// Following options are copied from Options struct.

	DB       int
	Password string

	MaxRetries int

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	PoolSize           int
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration
}

func (opt *RingOptions) init() {
	if opt.HeartbeatFrequency == 0 {
		opt.HeartbeatFrequency = 500 * time.Millisecond
	}

	opt.makeWeightLegal()
	opt.setNames(makeNames(opt.Weights))
	opt.index = 0
}

func (opt *RingOptions) makeWeightLegal() {
	// 去除不存在的name
	for name := range opt.Weights {
		if _, ok := opt.Addrs[name]; !ok {
			delete(opt.Weights, name)
		}
	}

	// 规范数据大小
	max := 0
	for name := range opt.Addrs {
		var w int
		var ok bool
		if w, ok = opt.Weights[name]; ok {
			if w <= 0 {
				// 小于等于0的节点放弃
				delete(opt.Weights, name)
				continue
			} else if w > nreplicas {
				w = nreplicas
			}
		} else {
			// 未配置，默认为nreplicas
			w = nreplicas
		}
		opt.Weights[name] = w
		if w > max {
			max = w
		}
	}

	// 最大权重提到nreplicas
	// 其它权重同比例提升
	// poll模式不需要提升，因为虚拟节点不起效
	if opt.PollMode && max < nreplicas {
		factor := float64(nreplicas) / float64(max)
		for name, w := range opt.Weights {
			opt.Weights[name] = int(math.Floor(factor*float64(w) + 0.5))
		}
	}
}

func (opt *RingOptions) setNames(names []string) {
	opt.namesLocker.Lock()
	opt.names = names
	opt.namesLocker.Unlock()
}

func (opt *RingOptions) nextNames() string {
	next := atomic.AddUint32(&opt.index, 1)
	opt.namesLocker.RLock()
	defer opt.namesLocker.RUnlock()
	if len(opt.names) == 0 {
		return ""
	}
	return opt.names[int(next)%len(opt.names)]
}

func (opt *RingOptions) clientOptions() *Options {
	return &Options{
		DB:       opt.DB,
		Password: opt.Password,

		DialTimeout:  opt.DialTimeout,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,

		PoolSize:           opt.PoolSize,
		PoolTimeout:        opt.PoolTimeout,
		IdleTimeout:        opt.IdleTimeout,
		IdleCheckFrequency: opt.IdleCheckFrequency,
	}
}

type ringShard struct {
	Client *Client
	down   int32
}

func (shard *ringShard) String() string {
	var state string
	if shard.IsUp() {
		state = "up"
	} else {
		state = "down"
	}
	return fmt.Sprintf("%s is %s", shard.Client, state)
}

func (shard *ringShard) IsDown() bool {
	return atomic.LoadInt32(&shard.down) >= threshold
}

func (shard *ringShard) IsUp() bool {
	return !shard.IsDown()
}

// Vote votes to set shard state and returns true if state was changed.
func (shard *ringShard) Vote(up bool) bool {
	if up {
		changed := shard.IsDown()
		atomic.StoreInt32(&shard.down, 0)
		return changed
	}

	if shard.IsDown() {
		return false
	}

	atomic.AddInt32(&shard.down, 1)
	return shard.IsDown()
}

// Ring is a Redis client that uses constistent hashing to distribute
// keys across multiple Redis servers (shards). It's safe for
// concurrent use by multiple goroutines.
//
// Ring monitors the state of each shard and removes dead shards from
// the ring. When shard comes online it is added back to the ring. This
// gives you maximum availability and partition tolerance, but no
// consistency between different shards or even clients. Each client
// uses shards that are available to the client and does not do any
// coordination when shard state is changed.
//
// Ring should be used when you need multiple Redis servers for caching
// and can tolerate losing data when one of the servers dies.
// Otherwise you should use Redis Cluster.
type Ring struct {
	cmdable

	opt       *RingOptions
	nreplicas int

	mu     sync.RWMutex
	hash   *consistenthash.Map
	shards map[string]*ringShard

	cmdsInfoOnce *sync.Once
	cmdsInfo     map[string]*CommandInfo

	closed bool
}

func NewRing(opt *RingOptions) *Ring {
	opt.init()
	ring := &Ring{
		opt:       opt,
		nreplicas: nreplicas,

		hash:   consistenthash.New(nreplicas, nil),
		shards: make(map[string]*ringShard),

		cmdsInfoOnce: new(sync.Once),
	}
	ring.cmdable.process = ring.Process

	for name, addr := range opt.Addrs {
		clopt := opt.clientOptions()
		clopt.Addr = addr
		ring.addClientWithWeight(opt.Weights[name], name, NewClient(clopt))
	}
	go ring.heartbeat()
	return ring
}

// Options returns read-only Options that were used to create the client.
func (c *Ring) Options() *RingOptions {
	return c.opt
}

// PoolStats returns accumulated connection pool stats.
func (c *Ring) PoolStats() *PoolStats {
	var acc PoolStats
	for _, shard := range c.shards {
		s := shard.Client.connPool.Stats()
		acc.Requests += s.Requests
		acc.Hits += s.Hits
		acc.Timeouts += s.Timeouts
		acc.TotalConns += s.TotalConns
		acc.FreeConns += s.FreeConns
	}
	return &acc
}

// ForEachShard concurrently calls the fn on each live shard in the ring.
// It returns the first error if any.
func (c *Ring) ForEachShard(fn func(client *Client) error) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	for _, shard := range c.shards {
		if shard.IsDown() {
			continue
		}

		wg.Add(1)
		go func(shard *ringShard) {
			defer wg.Done()
			err := fn(shard.Client)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(shard)
	}
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (c *Ring) cmdInfo(name string) *CommandInfo {
	c.cmdsInfoOnce.Do(func() {
		for _, shard := range c.shards {
			cmdsInfo, err := shard.Client.Command().Result()
			if err == nil {
				c.cmdsInfo = cmdsInfo
				return
			}
		}
		c.cmdsInfoOnce = &sync.Once{}
	})
	if c.cmdsInfo == nil {
		return nil
	}
	return c.cmdsInfo[name]
}

func (c *Ring) addClient(name string, cl *Client) {
	c.mu.Lock()
	c.hash.Add(name)
	c.shards[name] = &ringShard{Client: cl}
	c.mu.Unlock()
}

func (c *Ring) addClientWithWeight(weight int, name string, cl *Client) {
	c.mu.Lock()
	c.hash.AddWithReplicas(weight, name)
	c.shards[name] = &ringShard{Client: cl}
	c.mu.Unlock()
}

func (c *Ring) shardByKey(key string) (*ringShard, error) {
	key = hashtag.Key(key)

	if c.opt.MoveShards {
		c.mu.RLock()
	}

	if c.closed {
		if c.opt.MoveShards {
			c.mu.RUnlock()
		}
		return nil, pool.ErrClosed
	}

	name := c.hash.Get(key)
	if name == "" {
		if c.opt.MoveShards {
			c.mu.RUnlock()
		}
		return nil, errRingShardsDown
	}

	shard := c.shards[name]
	if c.opt.MoveShards {
		c.mu.RUnlock()
	}
	return shard, nil
}

func (c *Ring) randomShard() (*ringShard, error) {
	return c.shardByKey(strconv.Itoa(rand.Int()))
}

func (c *Ring) nextShard() (*ringShard, error) {
	name := c.opt.nextNames()
	if name == "" {
		return nil, errRingShardsDown
	}
	return c.shardByName(name)
}

func (c *Ring) shardByName(name string) (*ringShard, error) {
	if name == "" {
		return c.randomShard()
	}

	if c.opt.MoveShards {
		c.mu.RLock()
	}
	shard := c.shards[name]
	if c.opt.MoveShards {
		c.mu.RUnlock()
	}
	return shard, nil
}

func (c *Ring) cmdShard(cmd Cmder) (*ringShard, error) {
	cmdInfo := c.cmdInfo(cmd.name())
	firstKey := cmd.arg(cmdFirstKeyPos(cmd, cmdInfo))
	return c.shardByKey(firstKey)
}

func (c *Ring) Process(cmd Cmder) (e error) {
	var shard *ringShard
	var err error

	// 轮询模式
	for i := 0; i < 8*threshold; i++ {
		if c.opt.PollMode {
			shard, err = c.nextShard()
		} else {
			shard, err = c.cmdShard(cmd)
		}
		if err != nil {
			cmd.setErr(err)
			break
		}
		err = shard.Client.Process(cmd)
		if err == nil || err == Nil {
			break
		}
		if shard.Vote(false) {
			c.rebalance()
		}
	}
	return err
}

// rebalance removes dead shards from the Ring.
func (c *Ring) rebalance() {
	hash := consistenthash.New(c.nreplicas, nil)
	n2w := map[string]int{}
	for name, shard := range c.shards {
		if shard.IsUp() || !c.opt.MoveShards {
			n2w[name] = c.opt.Weights[name]
			hash.AddWithReplicas(c.opt.Weights[name], name)
		}
	}
	c.opt.setNames(makeNames(n2w))
	c.mu.Lock()
	c.hash = hash
	c.mu.Unlock()
}

// heartbeat monitors state of each shard in the ring.
func (c *Ring) heartbeat() {
	ticker := time.NewTicker(time.Millisecond * 500)
	defer ticker.Stop()
	for _ = range ticker.C {
		var rebalance bool

		if c.opt.MoveShards {
			c.mu.RLock()
		}

		if c.closed {
			if c.opt.MoveShards {
				c.mu.RUnlock()
			}
			break
		}

		for _, shard := range c.shards {
			err := shard.Client.Ping().Err()
			if shard.Vote(err == nil || err == pool.ErrPoolTimeout) {
				internal.Logf("ring shard state changed: %s", shard)
				rebalance = true
			}
		}

		if c.opt.MoveShards {
			c.mu.RUnlock()
		}

		if rebalance {
			c.rebalance()
		}
	}
}

// Close closes the ring client, releasing any open resources.
//
// It is rare to Close a Ring, as the Ring is meant to be long-lived
// and shared between many goroutines.
func (c *Ring) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	var firstErr error
	for _, shard := range c.shards {
		if err := shard.Client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.hash = nil
	c.shards = nil

	return firstErr
}

func (c *Ring) Pipeline() *Pipeline {
	pipe := Pipeline{
		exec: c.pipelineExec,
	}
	pipe.cmdable.process = pipe.Process
	pipe.statefulCmdable.process = pipe.Process
	return &pipe
}

func (c *Ring) Pipelined(fn func(*Pipeline) error) ([]Cmder, error) {
	return c.Pipeline().pipelined(fn)
}

func (c *Ring) pipelineExec(cmds []Cmder) (firstErr error) {
	cmdsMap := make(map[string][]Cmder)
	for _, cmd := range cmds {
		var name string
		if c.opt.PollMode {
			// 轮询
			name = c.opt.nextNames()
		} else {
			cmdInfo := c.cmdInfo(cmd.name())
			name = cmd.arg(cmdFirstKeyPos(cmd, cmdInfo))
			if name != "" {
				name = c.hash.Get(hashtag.Key(name))
			}
		}
		cmdsMap[name] = append(cmdsMap[name], cmd)
	}

	for i := 0; i <= c.opt.MaxRetries; i++ {
		var failedCmdsMap map[string][]Cmder

		for name, cmds := range cmdsMap {
			shard, err := c.shardByName(name)
			if err != nil {
				setCmdsErr(cmds, err)
				if firstErr == nil {
					firstErr = err
				}
				continue
			}

			cn, _, err := shard.Client.conn()
			if err != nil {
				setCmdsErr(cmds, err)
				if firstErr == nil {
					firstErr = err
				}
				continue
			}

			canRetry, err := shard.Client.pipelineProcessCmds(cn, cmds)
			shard.Client.putConn(cn, err, false)
			if err == nil {
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
			if canRetry && internal.IsRetryableError(err) {
				if failedCmdsMap == nil {
					failedCmdsMap = make(map[string][]Cmder)
				}
				failedCmdsMap[name] = cmds
			}
		}

		if len(failedCmdsMap) == 0 {
			break
		}
		cmdsMap = failedCmdsMap
	}

	return firstErr
}

// makeNames 通过权重，获取name的随机队列
func makeNames(n2w map[string]int) []string {
	// 求出所有数的最大公约数gcd
	first := true
	g := 0
	for _, w := range n2w {
		if first {
			g = w
			first = false
			continue
		}
		g = gcd(g, w)
		if g <= 1 {
			// 不需要再求，直接跳出
			break
		}
	}
	if g < 1 {
		g = 1
	}

	// 每个name有n/gcd个，组成一个共同的数组
	names := []string{}
	for name, w := range n2w {
		for i := 0; i < w/g; i++ {
			names = append(names, name)
		}
	}

	// 数组乱序
	rand.Seed(time.Now().Unix())
	for last := len(names) - 1; last > 0; last-- {
		num := rand.Intn(last)
		names[last], names[num] = names[num], names[last]
	}
	return names
}

func gcd(i, j int) int {
	// 最大公约数
	if i < j {
		i, j = j, i
	}
	if j == 0 {
		return i
	}
	return gcd(j, i%j)
}
