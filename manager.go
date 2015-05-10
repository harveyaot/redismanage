package redismanage

import (
	"code.google.com/p/gcfg"
	"errors"
	"fmt"
	"sync"
)

// redis query requst struct
type Request struct {
	Method string
	Params []interface{}
	Done   chan bool
	Reply  interface{}
	Err    error
	next   *Request
}

type Manager struct {
	clients []*client
	reqLock sync.Mutex //protect the
	freeReq *Request
}

type InvalidRequestError struct {
	What string
}

func NewManager(cfgFile string) (m *Manager, err error) {
	m = &Manager{}
	err = m.initConf(cfgFile)
	return
}

func (e InvalidRequestError) Error() string {
	return fmt.Sprintf("%v: %v", "Unvalid Redis Query Request:", e.What)
}

func (man *Manager) GetRequest(method string, params []interface{}) (req *Request) {
	req = man.getRequest()
	req.Method = method
	req.Params = params
	req.Done = make(chan bool, 1)
	return
}

func (man *Manager) DealRequest(req *Request) {
	// if param is not qualified
	if len(req.Params) < 1 {
		req.Err = InvalidRequestError{"Zero Parameters in Request!"}
		req.Done <- true
		return
	}

	// if there is no redis clients initialization
	if len(man.clients) == 0 {
		req.Err = errors.New("Non redis clients exists error")
		req.Done <- true
		return
	}

	// push request to client's queue
	str := req.Params[0]
	key, ok := str.(string)
	if !ok {
		req.Err = InvalidRequestError{"The first parameter should key of type string"}
		req.Done <- true
	}
	client := man.getClient(key)
	client.queue <- req
}

func (man *Manager) createClients(cfg config) (err error) {
	var errPre string = "Redis manage config error:"

	if len(cfg.RedisServer) == 0 {
		err = errors.New(errPre + "can't find redis servers")
		return
	}

	clients := make([]*client, len(cfg.RedisServer))
	var wg sync.WaitGroup
	for _, srvCfg := range cfg.RedisServer {
		if srvCfg.Idx >= len(clients) {
			err = errors.New(errPre + "idx larger than redis server's total num.")
			return
		}
		if clients[srvCfg.Idx] != nil {
			err = errors.New(errPre + "have duplicate redis server idx" + string(srvCfg.Idx))
			return
		}
		wg.Add(1)
		// concurrently create clients
		go func(srvCfg redisServerConf, poolCfg redisPoolConf) {
			defer wg.Done()
			clt := &client{}
			clt.initClient(srvCfg, poolCfg)
			clients[srvCfg.Idx] = clt
		}(*srvCfg, cfg.RedisPool)
	}
	wg.Wait()
	man.clients = clients
	return
}

// get a specific client for the coming Request
func (man *Manager) getClient(key string) *client {
	return man.clients[man.hashkey(key, len(man.clients))]
}

// get hash value of the key
func (man *Manager) hashkey(key string, count int) (idx uint32) {
	if key == "" || len(key) < 0 {
		idx = 0
		return
	}
	var (
		hash uint32 = 0
		seed uint32 = 131
	)

	for i := 0; i < len(key); i++ {
		hash = hash*seed + uint32(key[i])
	}
	idx = (hash & 0x7fffffff) % uint32(count)
	return
}

func (man *Manager) initConf(cfgFile string) (err error) {
	var cfg config
	err = gcfg.ReadFileInto(&cfg, cfgFile)
	if err != nil {
		return
	}
	err = man.createClients(cfg)
	return
}

func (man *Manager) getRequest() *Request {
	man.reqLock.Lock()
	req := man.freeReq
	if req == nil {
		req = new(Request)
	} else {
		man.freeReq = req.next
		*req = Request{}
	}
	man.reqLock.Unlock()
	return req
}

func (man *Manager) FreeRequest(req *Request) {
	man.reqLock.Lock()
	req.next = man.freeReq
	man.freeReq = req
	man.reqLock.Unlock()
}
