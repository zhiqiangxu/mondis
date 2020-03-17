package config

import "time"

var config = Value{
	WorkerMaxTickInterval: time.Second,
	Lease:                 0,
}
