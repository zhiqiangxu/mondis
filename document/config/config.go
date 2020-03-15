package config

import "time"

// Value for document db configuration
type Value struct {
	ReloadMaxTickInterval time.Duration
	WorkerMaxTickInterval time.Duration
	Lease                 time.Duration
}

// Load config
func Load() *Value {
	return &config
}
