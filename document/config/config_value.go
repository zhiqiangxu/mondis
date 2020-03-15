package config

import "time"

var config = Value{
	ReloadMaxTickInterval: time.Second,
	WorkerMaxTickInterval: time.Second,
	Lease:                 0,
}
