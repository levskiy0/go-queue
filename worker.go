package go_queue

import (
	"github.com/levskiy0/go-queue/contract"
	"log/slog"
)

type Worker struct {
	concurrent  int
	connection  string
	machinery   *Machinery
	jobs        []contract.Job
	queue       string
	log         *slog.Logger
	rateLimit   *contract.RateLimit
	metrics     *metricsRegistry
	ownsMetrics bool
}

func NewWorker(connections *Connections, log *slog.Logger, concurrent int, connection string, jobs []contract.Job, queue string) *Worker {
	worker := newWorker(connections, log, concurrent, connection, jobs, queue, newMetricsRegistry(connections, log))
	worker.ownsMetrics = true
	return worker
}

func newWorker(connections *Connections, log *slog.Logger, concurrent int, connection string, jobs []contract.Job, queue string, metrics *metricsRegistry) *Worker {
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
		metrics:    metrics,
	}
}

func (receiver *Worker) WithRateLimit(rateLimit *contract.RateLimit) *Worker {
	receiver.rateLimit = rateLimit

	return receiver
}

func (receiver *Worker) Run() error {
	if receiver.ownsMetrics {
		defer receiver.metrics.close()
	}
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

	metrics := receiver.metrics.worker(receiver.connection, queue, receiver.concurrent)
	jobTasks, err := jobs2Tasks(receiver.jobs, receiver.log, queue, receiver.rateLimit, metrics)
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
	metrics.start()
	defer metrics.close()
	if err := worker.Launch(); err != nil {
		return err
	}

	return nil
}
