package go_queue

import (
	"log/slog"
	"sync"

	mlog "github.com/RichardKnop/machinery/v2/log"
	"github.com/levskiy0/go-queue/contract"
)

var machineryLogOnce sync.Once

type Queue struct {
	connections *Connections
	jobs        []contract.Job
	log         *slog.Logger
	machinery   *Machinery
}

func NewQueue(connections *Connections, log *slog.Logger, debug bool) *Queue {
	if log == nil {
		log = slog.Default()
	}
	machineryLogOnce.Do(func() {
		mlog.SetDebug(newMachineryLogger(log, slog.LevelDebug, debug))
		mlog.SetInfo(newMachineryLogger(log, slog.LevelInfo, true))
		mlog.SetWarning(newMachineryLogger(log, slog.LevelWarn, true))
		mlog.SetError(newMachineryLogger(log, slog.LevelError, true))
		mlog.SetFatal(newMachineryLogger(log, slog.LevelError, true))
	})

	return &Queue{
		connections: connections,
		log:         log,
		machinery:   NewMachinery(connections, log),
	}
}

func (q *Queue) Worker(args ...contract.Args) contract.Worker {
	defaultConnection := q.connections.GetDefault()

	if len(args) == 0 {
		return NewWorker(q.connections, q.log, 1, defaultConnection, q.jobs, "default")
	}

	if args[0].Connection == "" {
		args[0].Connection = defaultConnection
	}

	return NewWorker(q.connections, q.log, args[0].Concurrent, args[0].Connection, q.jobs, args[0].Queue).WithRateLimit(args[0].RateLimit)
}

func (q *Queue) Register(jobs []contract.Job) {
	q.jobs = append(q.jobs, jobs...)
}

func (q *Queue) GetJobs() []contract.Job {
	return q.jobs
}

func (q *Queue) Job(job contract.Job, args []contract.Arg) contract.Task {
	return newTask(q.connections, q.machinery, job, args)
}

func (q *Queue) Chain(jobs []contract.Jobs) contract.Task {
	return newChainTask(q.connections, q.machinery, jobs)
}
