package config

import "time"

// Timing constants tuned for local tests.
const (
	ElectionTimeoutMin = 300 * time.Millisecond
	ElectionTimeoutMax = 600 * time.Millisecond
	HeartbeatInterval  = 50 * time.Millisecond

	// Network timeouts
	RPCTimeout = 200 * time.Millisecond

	// Log retention
	RetainTail = 100
	PruneEvery = 2000
)
