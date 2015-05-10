package redismanage

import ()

type redisServerConf struct {
	Server     string
	Port       int
	Idx        int
	BatchSize  int
	WorkerSize int
	QueueSize  int
}

type redisPoolConf struct {
	MaxIdle     int
	MaxActive   int
	IdleTimeOut int
	ConnTimeOut int
	NeedWarmUp  bool
}

type config struct {
	RedisServer map[string]*redisServerConf
	RedisPool   redisPoolConf
}
