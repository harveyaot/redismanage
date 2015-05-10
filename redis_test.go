package redismanage

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"testing"
	//"time"
)

func TestHash(t *testing.T) {
	man, _ := NewManager("./conf/redismanage.conf")
	var key string
	if 0 != man.hashkey("ap:784476aba3a7", 6) {
		t.Error("error hash key")
	}
	if 1 != man.hashkey("ap:841b5e7a87a5", 6) {
		t.Error("error hash key")
	}

	key = "ap:0022aa0009b0"
	if 2 != man.hashkey(key, 6) {
		fmt.Println(man.hashkey(key, 6))
		t.Error("error hash key")
	}

	key = "poi:16493593072512832249"
	if 3 != man.hashkey(key, 6) {
		fmt.Println(man.hashkey(key, 6))
		t.Error("error hash key")
	}

	key = "ap:b8c716fd15de"
	if 5 != man.hashkey(key, 6) {
		fmt.Println(man.hashkey(key, 6))
		t.Error("error hash key")
	}
	key = "grid:827329094"
	if 0 != man.hashkey(key, 6) {
		fmt.Println(man.hashkey(key, 6))
		t.Error("error hash key")
	}

}

func TestDealRequest(t *testing.T) {
	test_val_1 := 1001
	manager, err := NewManager("./conf/redismanage.conf")

	req_set_1 := manager.GetRequest("HSET",
		[]interface{}{"test1", "1", test_val_1})

	test_val_2 := 1002
	req_set_2 := manager.GetRequest("HSET",
		[]interface{}{"test2", "1", test_val_2})

	if err != nil {
		t.Error(err)
		return
	}
	manager.DealRequest(req_set_1)
	manager.DealRequest(req_set_2)
	<-req_set_1.Done
	<-req_set_2.Done
	manager.FreeRequest(req_set_1)
	manager.FreeRequest(req_set_2)

	size := 3000
	reqs := make([]*Request, size)
	for i := 0; i < size; i++ {
		req := manager.GetRequest("HGET", []interface{}{"test2", "1"})
		reqs[i] = req
		//time.Sleep(50)
		manager.DealRequest(req)
	}

	for _, req := range reqs {
		<-req.Done
		manager.FreeRequest(req)
		val, err := redis.Int64(req.Reply, req.Err)
		if val != int64(test_val_2) {
			t.Errorf("The response val is %v", val)
			t.Errorf("%v", err)
			//if err != nil {
			//	b.Errorf("%v", err)
		}
	}

}

func TestHSETHGETmulti(t *testing.T) {

	manager, err := NewManager("./conf/redismanage.conf")
	if err != nil {
		t.Error(err)
	}

	test_val_1 := 1001
	req_set_1 := Request{"HSET",
		[]interface{}{"test1", "1", test_val_1},
		make(chan bool, 1),
		nil,
		nil,
		nil,
	}

	test_val_2 := 1002
	req_set_2 := Request{"HSET",
		[]interface{}{"test2", "1", test_val_2},
		make(chan bool, 1),
		nil,
		nil,
		nil,
	}

	manager.DealRequest(&req_set_1)
	manager.DealRequest(&req_set_2)
	<-req_set_1.Done
	<-req_set_2.Done

	size := 3000
	reqs := make([]*Request, size)
	for i := 0; i < size; i++ {
		req := &Request{"HGET",
			[]interface{}{"test2", "1"},
			make(chan bool, 1),
			nil,
			nil,
			nil,
		}
		reqs[i] = req
		manager.DealRequest(req)
	}

	for _, req := range reqs {
		<-req.Done
		val, err := redis.Int64(req.Reply, req.Err)
		if err != nil {
			t.Errorf("%v", err)
		} else {
			if val != int64(test_val_2) {
				t.Errorf("The response val is %v", val)
			}
		}
	}

}
