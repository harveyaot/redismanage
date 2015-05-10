# redismanage
redis cluster management

A lib based on [redigo](https://github.com/garyburd/redigo) add new features as follows:

1.  support for configing and managing redis cluster
2.  support for asyn request for redis
3.  support for pipline request and pipline handling requests under redis cluster situation

## Example

```go
    manager, err := NewManager("./conf/redismanage.conf")
    if err != nil{
        log.Println(err)
        return 
    }
    req_set_1 := manager.GetRequest("HSET",
        []interface{}{"test1", "1", test_val_1})
     manager.DealRequest(req_set_1)

    <-req_set_1.Done // aycn deal redis query request
    manager.FreeRequest(req_set_1)

```

