package contract

import "time"

type Queue interface {
	Worker(args ...Args) Worker
	// Register register jobs
	Register(jobs []Job)
	// GetJobs get all jobs
	GetJobs() []Job
	// Job add a job to queue
	Job(job Job, args []Arg) Task
	// Chain creates a chain of jobs to be processed one by one, passing
	Chain(jobs []Jobs) Task
}

type Worker interface {
	Run() error
}

type Args struct {
	// Specify connection
	Connection string
	// Specify queue
	Queue string
	// Concurrent num
	Concurrent int
	RateLimit  *RateLimit
}

type RateLimit struct {
	Limit int
	Per   time.Duration
	Key   func(args []any) string
}

type Arg struct {
	Type  string
	Value any
}
