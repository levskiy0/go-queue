package go_queue

import (
	"github.com/levskiy0/go-queue/contract"
	"log/slog"
)

type Worker struct {
	concurrent int
	connection string
	machinery  *Machinery
	jobs       []contract.Job
	queue      string
}

func NewWorker(connections *Connections, log *slog.Logger, concurrent int, connection string, jobs []contract.Job, queue string) *Worker {
	return &Worker{
		concurrent: concurrent,
		connection: connection,
		machinery:  NewMachinery(connections, log),
		jobs:       jobs,
		queue:      queue,
	}
}

func (receiver *Worker) Run() error {
	server, err := receiver.machinery.Server(receiver.connection, receiver.queue)
	if err != nil {
		return err
	}
	if server == nil {
		return nil
	}

	jobTasks, err := jobs2Tasks(receiver.jobs)
	if err != nil {
		return err
	}

	if err := server.RegisterTasks(jobTasks); err != nil {
		return err
	}

	if receiver.queue == "" {
		receiver.queue = server.GetConfig().DefaultQueue
	}
	if receiver.concurrent == 0 {
		receiver.concurrent = 1
	}
	worker := server.NewWorker(receiver.queue, receiver.concurrent)
	if err := worker.Launch(); err != nil {
		return err
	}

	return nil
}
