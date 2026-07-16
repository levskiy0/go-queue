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
	log        *slog.Logger
	rateLimit  *contract.RateLimit
}

func NewWorker(connections *Connections, log *slog.Logger, concurrent int, connection string, jobs []contract.Job, queue string) *Worker {
	if log == nil {
		log = slog.Default()
	}

	return &Worker{
		concurrent: concurrent,
		connection: connection,
		machinery:  NewMachinery(connections, log),
		jobs:       jobs,
		queue:      queue,
		log:        log,
	}
}

func (receiver *Worker) WithRateLimit(rateLimit *contract.RateLimit) *Worker {
	receiver.rateLimit = rateLimit

	return receiver
}

func (receiver *Worker) Run() error {
	server, err := receiver.machinery.Server(receiver.connection, receiver.queue)
	if err != nil {
		return err
	}
	if server == nil {
		return nil
	}

	queue := receiver.queue
	if queue == "" {
		queue = server.GetConfig().DefaultQueue
	}

	jobTasks, err := jobs2Tasks(receiver.jobs, receiver.log, queue, receiver.rateLimit)
	if err != nil {
		return err
	}

	if err := server.RegisterTasks(jobTasks); err != nil {
		return err
	}

	receiver.queue = queue
	if receiver.concurrent == 0 {
		receiver.concurrent = 1
	}
	worker := server.NewWorker(receiver.queue, receiver.concurrent)
	if err := worker.Launch(); err != nil {
		return err
	}

	return nil
}
