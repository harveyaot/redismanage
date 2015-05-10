package redismanage

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	//"log"
	"time"
)

const (
	MAXBATCHSIZE int = 200
)

type client struct {
	pool      *redis.Pool
	queue     chan *Request
	batchSize int
	idx       int
}

func (clt *client) initClient(srvCfg redisServerConf, poolCfg redisPoolConf) {
	pool := createPool(srvCfg, poolCfg)
	queue := make(chan *Request, srvCfg.QueueSize)
	clt.pool = pool
	clt.queue = queue
	clt.batchSize = srvCfg.BatchSize
	clt.idx = srvCfg.Idx

	// batch size can't exceed the max batch size
	if srvCfg.BatchSize > MAXBATCHSIZE {
		clt.batchSize = MAXBATCHSIZE
	}

	clt.createWorkers(srvCfg.WorkerSize)
}

func (clt *client) createQueryProcessor(id int) {
	go func() {

		var toDeal [MAXBATCHSIZE]*Request
		idx := 0
		for {
			req := <-clt.queue
			//log.Println(id, "goroutine is gonna work.")
			toDeal[idx] = req
			idx++

			// try to select batch size times
			for j := 0; j < clt.batchSize; j++ {
				select {
				case req := <-clt.queue:
					toDeal[idx] = req
					idx++
					if idx == clt.batchSize {
						goto DEAL
					}
				default:
					break
				}
			}

			// when toDeal has task to deal
		DEAL:
			if idx > 0 {
				clt.processQuery(toDeal, idx)
				//log.Printf("client idx is [%d], goroutine idx is [%d] processing num is [%d]\n", clt.idx, id, idx)
				idx = 0
			}
		}
	}()
}

// processQuery in pipline
func (clt *client) processQuery(reqs [MAXBATCHSIZE]*Request, size int) {
	// get a conn from pool
	conn := clt.pool.Get()
	defer conn.Close()
	// send reuqests
	for i := 0; i < size; i++ {
		conn.Send(reqs[i].Method, reqs[i].Params...)
	}
	conn.Flush()

	//receive response
	for i := 0; i < size; i++ {
		reqs[i].Reply, reqs[i].Err = conn.Receive()
		// mark the request as done
		reqs[i].Done <- true
	}
}

func (clt *client) createWorkers(size int) {

	for i := 0; i < size; i++ {
		clt.createQueryProcessor(i)
		// warm up redis connection
	}

}

func createPool(srvCfg redisServerConf, poolCfg redisPoolConf) *redis.Pool {
	var addr string = fmt.Sprintf("%s:%d", srvCfg.Server, srvCfg.Port)
	pool := &redis.Pool{
		MaxIdle:     poolCfg.MaxIdle,
		MaxActive:   poolCfg.MaxActive,
		IdleTimeout: time.Duration(poolCfg.IdleTimeOut) * time.Second,
		Dial: func() (redis.Conn, error) {
			c_timeout := time.Duration(poolCfg.ConnTimeOut) * time.Millisecond
			r_timeout := c_timeout
			w_timeout := c_timeout
			c, err := redis.DialTimeout("tcp", addr, c_timeout, r_timeout, w_timeout)
			//c, err := redis.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}
	if poolCfg.NeedWarmUp {
		for i := 0; i < srvCfg.WorkerSize; i++ {
			conn := pool.Get()
			defer conn.Close()
		}
	}
	return pool
}
