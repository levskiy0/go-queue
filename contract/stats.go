package contract

import (
	"context"
	"time"
)

type StatsReader interface {
	Stats(ctx context.Context, connection ...string) (Stats, error)
}

type Stats struct {
	StartedAt     time.Time    `json:"started_at"`
	WorkersActive int64        `json:"workers_active"`
	WorkersTotal  int64        `json:"workers_total"`
	Processed     uint64       `json:"processed"`
	Succeeded     uint64       `json:"succeeded"`
	Failed        uint64       `json:"failed"`
	Pending       int64        `json:"pending"`
	Queues        []QueueStats `json:"queues"`
}

type QueueStats struct {
	Name          string `json:"name"`
	WorkersActive int64  `json:"workers_active"`
	WorkersTotal  int64  `json:"workers_total"`
	Processed     uint64 `json:"processed"`
	Succeeded     uint64 `json:"succeeded"`
	Failed        uint64 `json:"failed"`
	Pending       int64  `json:"pending"`
}
