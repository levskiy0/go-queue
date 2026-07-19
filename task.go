package go_queue

import (
	"fmt"
	"github.com/RichardKnop/machinery/v2"
	"github.com/levskiy0/go-queue/contract"
	"log/slog"
	"time"

	"github.com/RichardKnop/machinery/v2/tasks"
)

type Task struct {
	connections  *Connections
	connection   string
	chain        bool
	delay        *time.Time
	machinery    *Machinery
	jobs         []contract.Jobs
	queue        string
	server       *machinery.Server
	retries      int
	retryTimeout time.Duration
}

func NewTask(connections *Connections, log *slog.Logger, job contract.Job, args []contract.Arg) *Task {
	return newTask(connections, NewMachinery(connections, log), job, args)
}

func newTask(connections *Connections, machinery *Machinery, job contract.Job, args []contract.Arg) *Task {
	return &Task{
		connections: connections,
		connection:  connections.GetDefault(),
		machinery:   machinery,
		jobs: []contract.Jobs{
			{
				Job:  job,
				Args: args,
			},
		},
	}
}

func NewChainTask(connections *Connections, log *slog.Logger, jobs []contract.Jobs) *Task {
	return newChainTask(connections, NewMachinery(connections, log), jobs)
}

func newChainTask(connections *Connections, machinery *Machinery, jobs []contract.Jobs) *Task {
	return &Task{
		connections: connections,
		connection:  connections.GetDefault(),
		chain:       true,
		machinery:   machinery,
		jobs:        jobs,
	}
}

func (receiver *Task) Delay(delay time.Time) contract.Task {
	receiver.delay = &delay

	return receiver
}

func (receiver *Task) Dispatch() error {
	conn := receiver.connections.Get(receiver.connection)
	if conn == nil {
		return fmt.Errorf("cannot find default connection")
	}

	if conn.Driver == DriverSync {
		return receiver.DispatchSync()
	}

	server, err := receiver.machinery.Producer(receiver.connection)
	if err != nil {
		return err
	}

	receiver.server = server

	if receiver.chain {
		return receiver.handleChain(receiver.jobs)
	} else {
		job := receiver.jobs[0]

		return receiver.handleAsync(job.Job, job.Args)
	}
}

func (receiver *Task) DispatchSync() error {
	if receiver.chain {
		for _, job := range receiver.jobs {
			if err := receiver.handleSync(job.Job, job.Args); err != nil {
				return err
			}
		}

		return nil
	} else {
		job := receiver.jobs[0]

		return receiver.handleSync(job.Job, job.Args)
	}
}

func (receiver *Task) OnConnection(connection string) contract.Task {
	receiver.connection = connection

	return receiver
}

func (receiver *Task) OnQueue(queue string) contract.Task {
	receiver.queue = queue

	return receiver
}

func (receiver *Task) Retries(count int) contract.Task {
	receiver.retries = count

	return receiver
}

func (receiver *Task) RetryAfter(initial time.Duration) contract.Task {
	receiver.retryTimeout = initial

	return receiver
}

func (receiver *Task) handleChain(jobs []contract.Jobs) error {
	var signatures []*tasks.Signature
	for _, job := range jobs {
		var realArgs []tasks.Arg
		for _, arg := range job.Args {
			realArgs = append(realArgs, tasks.Arg{
				Type:  arg.Type,
				Value: arg.Value,
			})
		}

		signatures = append(signatures, &tasks.Signature{
			Name:         job.Job.Signature(),
			Args:         realArgs,
			RoutingKey:   receiver.routingKey(),
			ETA:          receiver.delay,
			RetryCount:   receiver.retries,
			RetryTimeout: retryTimeoutSeconds(receiver.retryTimeout),
		})
	}

	chain, err := tasks.NewChain(signatures...)
	if err != nil {
		return err
	}

	_, err = receiver.server.SendChain(chain)

	return err
}

func (receiver *Task) handleAsync(job contract.Job, args []contract.Arg) error {
	var realArgs []tasks.Arg
	for _, arg := range args {
		realArgs = append(realArgs, tasks.Arg{
			Type:  arg.Type,
			Value: arg.Value,
		})
	}

	_, err := receiver.server.SendTask(&tasks.Signature{
		Name:         job.Signature(),
		Args:         realArgs,
		RoutingKey:   receiver.routingKey(),
		ETA:          receiver.delay,
		RetryCount:   receiver.retries,
		RetryTimeout: retryTimeoutSeconds(receiver.retryTimeout),
	})
	if err != nil {
		return err
	}

	return nil
}

func (receiver *Task) routingKey() string {
	if receiver.queue == "" {
		return "default"
	}

	return receiver.queue
}

func retryTimeoutSeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}

	seconds := d / time.Second
	if d%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}

	return int(seconds)
}

func (receiver *Task) handleSync(job contract.Job, args []contract.Arg) error {
	var realArgs []any
	for _, arg := range args {
		realArgs = append(realArgs, arg.Value)
	}

	return job.Handle(realArgs...)
}
